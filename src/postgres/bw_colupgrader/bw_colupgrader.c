/*
 * TODO: insert PG copyright header
 * Copyright Hadapt, Inc. 2013
 */
#include <postgres.h> /* This must precede all other includes */

#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>

#include <executor/spi.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <utils/snapmgr.h>
// FIXME: Do i use these?
#include <access/xact.h>
#include <pgstat.h>
#include <utils/builtins.h>
#include <tcop/utility.h>

PG_MODULE_MAGIC;

void _PG_init(void);
static bool upgrade_columns(char *tname);
static bool downgrade_columns(char *tname);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	bw_colupgrader_naptime = 10;
static int	bw_colupgrader_total_workers = 1;

#define SCHEMA_NAME "document_schema"
#define BGW_MAXLEN (64)

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
 */
static void
bw_colupgrader_sigterm(SIGNAL_ARGS)
{
	int	save_errno = errno;

	got_sigterm = true;
	if (MyProc)
	{
		SetLatch(&MyProc->procLatch);
    }

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 * Set a flag to let the main loop to reread the config file, and set
 * our latch to wake it up.
 */
static void
bw_colupgrader_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
	{
		SetLatch(&MyProc->procLatch);
    }
}

/*
 * Initialize workspace for a worker process: create the schema if it doesn't
 * already exist.
 */
static void
initialize_bw_colupgrader()
{
    int	  ret;
    int	  ntup;
    bool  isnull;
    StringInfoData buf;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_activity(STATE_RUNNING, "bw_colupgrader: initializing document schema");

    /* XXX could we use CREATE SCHEMA IF NOT EXISTS? */
    initStringInfo(&buf);
    appendStringInfo(&buf, "select count(*) from pg_namespace where nspname = 'document_schema'");

    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "bw_colupgrader: SPI_execute failed: error code %d", ret);
    }

    if (SPI_processed != 1)
    {
        elog(ERROR, "bw_colupgrader: not a singleton result");
    }

    ntup = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                       SPI_tuptable->tupdesc,
                       1, &isnull));
    if (isnull)
    {
        elog(ERROR, "bw_colupgrader: null result");
    }

    if (ntup == 0)
    {
        resetStringInfo(&buf);
        appendStringInfo(&buf, "CREATE SCHEMA document_schema");

        /* set statement start time */
        SetCurrentStatementStartTimestamp();

        ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_UTILITY)
        {
            elog(FATAL, "bw_colupgrader: failed to create my schema");
        }

        resetStringInfo(&buf);
        appendStringInfo(&buf, "CREATE TABLE document_schema._attributes(_id SERIAL, key_name TEXT, key_type TEXT");

        ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_UTILITY)
        {
            elog(FATAL, "bw_colupgrader: failed to create document_schema._attributes");
        }
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);
}

static bool
upgrade_columns(char *tname)
{
    StringInfoData buf;

    initStringInfo(&buf);

    /* Retrieve document schema for table */
    appendStringInfo(&buf,
                     "SELECT key_id FROM %s.%s WHERE "
                     "upgraded = true AND dirty = true",
                     SCHEMA_NAME,
                     tname);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "bw_colupgrade: cannot get document schema for table '%s'", tname);
    }
    else /* Materialize a column */
    {
        char *key_name;
        char *key_type;
        char *udf_suffix;
        int attr_id;
        bool isnull;
        int ntups;
        int *attr_ids;
        int j;
        bool exists;

        ntups = SPI_processed;
        if (ntups == 0) /* Go until we find a table with a column to upgrade, or we run out of tables */
        {
            elog(DEBUG5, "bw_colupgrade: table '%s' has no upgradeable columns", tname);
            return false;
        }

        attr_ids = palloc0(ntups * sizeof(int));
        for (j = 0; j < ntups; j++)
        {
              attr_id[j] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[j],
                                                       SPI_tuptable->tupdesc,
                                                       1,
                                                       &isnull));
              if (isnull)
              {
                  elog(WARNING, "bw_colupgrade: null result for attribute id");
                  return false;
              }
        }

        for (j = 0; j < ntups; j++)
        {
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "SELECT key_name, key_type FROM document_schema._attributes WHERE"
                               " _id = %d",
                               attr_id);
              ret = SPI_execute(buf.data, true, 0);
              if (ret != SPI_OK_SELECT)
              {
                  elog(ERROR, "bw_colupgrade: cannot find attribute name for id %d", attr_id);
              }
              if (SPI_processed == 0) /* Go until we find a table with a column to upgrade, or we run out of tables */
              {
                  elog(ERROR, "bw_colupgrade: cannot find attribute name for id %d", attr_id);
                  return false;
              }

              key_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
              key_type = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

              /* See if column exists */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "SELECT 1 FROM pg_attribute WHERE attrelid = '%s'::regclass "
                               "AND attrname = %s AND NOT attisdropped",
                               tname,
                               key_name);
              ret = SPI_execute(buf.data, true, 0);
              if (ret != SPI_OK_SELECT)
              {
                  elog(ERROR, "bw_colupgrade: checking column existence failed")
              }
              /* Need to create column */
              if (SPI_processed == 0)
              {
                  resetStringInfo(&buf);
                  appendStringInfo(&buf,
                                   "ALTER TABLE %s ADD COLUMN %s %s",
                                   tname,
                                   key_name,
                                   key_type);
                  ret = SPI_execute(buf.data, false, 0);
                  if (ret != SPI_OK_UTILITY)
                  {
                      elog(ERROR, "bw_colupgrade: column creation for '%s.%s' failed", tname, key_name);
                  }
              }

              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "UPDATE %s SET %s = document_get(json_data, '%s', '%s')",
                               tname,
                               key_name,
                               key_name,
                               key_type);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UPDATE)
              {
                  elog(ERROR, "bw_colupgrade: column upgrade for '%s.%s' failed", tname, key_name);
              }
              /* Delete from doc column */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "UPDATE %s SET json_data = document_delete(json_data, '%s', '%s')",
                               tname,
                               key_name,
                               key_type);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UPDATE)
              {
                  elog(ERROR, "bw_colupgrade: column upgrade for '%s.%s' failed", tname, key_name);
              }

              /* Set dirty = false */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "UPDATE %s.%s SET dirty = false" 
                               " WHERE _id = %d",
                               SCHEMA_NAME,
                               tname,
                               attr_id);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UPDATE || SPI_processed != 1)
              {
                  elog(ERROR, "bw_colupgrade: could not update schema properly");
              }
        }
        pfree(attr_ids);
        return true;
    }
}

static bool
downgrade_columns(char *tname)
{
    StringInfoData buf;

    initStringInfo(&buf);

    /* Retrieve document schema for table */
    appendStringInfo(&buf,
                     "SELECT key_id FROM %s.%s WHERE "
                     "upgraded = true AND dirty = true",
                     SCHEMA_NAME,
                     tname);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "bw_colupgrade: cannot get document schema for table '%s'", tname);
    }
    else /* Materialize a column */
    {
        char *key_name;
        char *key_type;
        char *udf_suffix;
        int attr_id;
        bool isnull;
        int ntups;
        int *attr_ids;
        int j;
        bool exists;

        ntups = SPI_processed;
        if (ntups == 0) /* Go until we find a table with a column to upgrade, or we run out of tables */
        {
            elog(DEBUG5, "bw_colupgrade: table '%s' has no upgradeable columns", tname);
            return false;
        }

        attr_ids = palloc0(ntups * sizeof(int));
        for (j = 0; j < ntups; j++)
        {
              attr_id[j] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[j],
                                                       SPI_tuptable->tupdesc,
                                                       1,
                                                       &isnull));
              if (isnull)
              {
                  elog(WARNING, "bw_colupgrade: null result for attribute id");
                  return false;
              }
        }

        for (j = 0; j < ntups; j++)
        {
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "SELECT key_name, key_type FROM document_schema._attributes WHERE"
                               " _id = %d",
                               attr_id);
              ret = SPI_execute(buf.data, true, 0);
              if (ret != SPI_OK_SELECT)
              {
                  elog(ERROR, "bw_colupgrade: cannot find attribute name for id %d", attr_id);
              }
              if (SPI_processed == 0) /* Go until we find a table with a column to upgrade, or we run out of tables */
              {
                  elog(ERROR, "bw_colupgrade: cannot find attribute name for id %d", attr_id);
                  return false;
              }

              key_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
              key_type = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

              /*
               * NOTE: assume that if we have upgraded == FALSE && dirty, then
               * the column exists. Otherwise, we have bigger problems than
               * this module
               */
              /* Add to doc column */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "UPDATE %s SET json_data = document_put(json_data, '%s', '%s', %s)",
                               tname,
                               key_name,
                               key_type,
                               key_name);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UPDATE)
              {
                  elog(ERROR, "bw_colupgrade: column upgrade for '%s.%s' failed", tname, key_name);
              }

              /* Delete current column */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "ALTER TABLE %s DROP COLUMN %s",
                               tname,
                               key_name);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UTILITY)
              {
                  elog(ERROR, "bw_colupgrade: column deletion for '%s.%s' failed", tname, key_name);
              }

              /* Set dirty = false */
              resetStringInfo(&buf);
              appendStringInfo(&buf,
                               "UPDATE %s.%s SET dirty = false" 
                               " WHERE _id = %d",
                               SCHEMA_NAME,
                               tname,
                               attr_id);
              ret = SPI_execute(buf.data, false, 0);
              if (ret != SPI_OK_UPDATE || SPI_processed != 1)
              {
                  elog(ERROR, "bw_colupgrade: could not update schema properly");
              }
        }
        pfree(attr_ids);
        return true;
    }
}
void
bw_colupgrader_main(void *main_arg)
{
    StringInfoData buf;

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to our database */
    BackgroundWorkerInitializeConnection("test", "postgres");

    initialize_bw_colupgrader();

    while (!got_sigterm)
    {
        int	ret;
        int	rc;
        int num_tables;

        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       bw_colupgrader_naptime * 1000L);
        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
        {
            proc_exit(1);
        }

        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, buf.data);

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT tablename FROM pg_tables WHERE schemaname='%s'", SCHEMA_NAME);

        /* We can now execute queries via SPI */
        ret = SPI_execute(buf.data, true, 0);

        if (ret != SPI_OK_SELECT)
        {
            elog(ERROR, "bw_colupgrade: cannot get table names in schema '%s'", SCHEMA_NAME);
        }

        num_tables = SPI_processed;
        if (num_tables > 0)
        {
            int     i;
            char  **tnames;

            tnames = palloc0(num_tables * sizeof(char*));
            for (i = 0; i < num_tables; i++)
            {
                tnames[i] = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            }

            /* Need to do this separately so as not to overwrite the global SPI_tuptable */
            for (i = 0; i < num_tables; i++)
            {
                char *tname;
                // FIXME: always ask for a lock on this table; if can't acquire, sleep

                resetStringInfo(&buf);
                tname = tnames[i];
                if (!tname || !strcmp(tname, "_attributes")
                {
                    continue;
                }

                if (upgradeColumns(tname) || downgradeColumns(tname))
                {
                    break;
                }
            }
        }

        /*
         * And finish our transaction.
         */
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity(STATE_IDLE, NULL);
    }

    proc_exit(0);
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
    BackgroundWorker worker;
    unsigned int i;

    /* get the configuration */
    DefineCustomIntVariable("bw_colupgrader.naptime",
                            "Duration between each check (in seconds).",
                            NULL,
                            &bw_colupgrader_naptime,
                            10,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    if (!process_shared_preload_libraries_in_progress)
    {
        return;
    }

    DefineCustomIntVariable("bw_colupgrader.total_workers",
                            "Number of workers.",
                            NULL,
                            &bw_colupgrader_total_workers,
                            1,
                            1,
                            100,
                            PGC_POSTMASTER,
                            0,
                            NULL,
                            NULL,
                            NULL);

    /* set up common data for all our workers */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main = bw_colupgrader_main;
    worker.bgw_sighup = bw_colupgrader_sighup;
    worker.bgw_sigterm = bw_colupgrader_sigterm;
    worker.bgw_main_arg = NULL;

    /*
     * Now fill in worker-specific data, and do the actual registrations.
     */
    for (i = 1; i <= bw_colupgrader_total_workers; i++)
    {
        char name[BGW_MAXLEN];

        sprintf(name, "bw_colupgrader_%d", i);
        worker.bgw_name = pstrdup(name);

        RegisterBackgroundWorker(&worker);
    }
}

/*
 * Dynamically launch an SPI worker.
 * NOTE: Changes as of 7/16/13
 * https://github.com/postgres/postgres/commit/7f7485a0cde92aa4ba235a1ffe4dda0ca0b6cc9a
 * Datum
 * bw_colupgrader_launch(PG_FUNCTION_ARGS)
 * {
 *     BackgroundWorker worker;
 *
 *     worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
 *       BGWORKER_BACKEND_DATABASE_CONNECTION;
 *     worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
 *     worker.bgw_restart_time = 300;
 *     worker.bgw_main = NULL;		// new worker might not have library loaded
 *     sprintf(worker.bgw_library_name, "bw_colupgrader");
 *     sprintf(worker.bgw_function_name, "bw_colupgrader_main");
 *     worker.bgw_sighup = NULL;	// new worker might not have library loaded
 *     worker.bgw_sigterm = NULL;	// new worker might not have library loaded
 *     worker.bgw_main_arg = PointerGetDatum(NULL);
 *
 *     PG_RETURN_BOOL(RegisterDynamicBackgroundWorker(&worker));
 * }
 */
