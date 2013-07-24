/* -------------------------------------------------------------------------
 *
 * bw_colupgrader.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and summarizes
 * the numbers contained therein.  To see it working, insert an initial value
 * with "total" type and some initial value; then insert some other rows with
 * "delta" type.  Delta rows will be deleted by this worker and their values
 * aggregated into the total.
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bw_colupgrader/bw_colupgrader.c
 *
 * -------------------------------------------------------------------------
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

void    _PG_init(void);

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
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
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
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
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
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
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

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int	ret;
		int	rc;
        int num_tables;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   bw_colupgrader_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
        {
            proc_exit(1);
        }

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we're about the run, and also the transaction
		 * start time.	Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 * The pgstat_report_activity() call makes our activity visible
		 * through the pgstat views.
		 */
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
            for (i = 0; i < num_tables; i++) {
                tnames[i] = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            }

            /* Need to do this separately so as not to overwrite the global SPI_tuptable */
            for (i = 0; i < num_tables; i++) {
                char *tname;

                resetStringInfo(&buf);
                tname = tnames[i];
                if (!tname)
                {
                    continue;
                }

                /* Retrieve document schema for table */
                appendStringInfo(&buf,
                                 "SELECT key_name, key_type FROM %s.%s WHERE "
                                 "materialized = true AND dirty = true",
                                 SCHEMA_NAME,
                                 tname);
                ret = SPI_execute(buf.data, true, 0);
                if (ret != SPI_OK_SELECT)
                {
                    elog(ERROR, "bw_colupgrade: cannot get document schema for table '%s'", tname);
                }
                if (SPI_processed == 0) /* Go until we find a table with a column to upgrade, or we run out of tables */
                {
                    elog(DEBUG5, "bw_colupgrade: table '%s' has no upgradeable columns", tname);
                    continue;
                }
                else /* Materialize a column */
                {
                    char *key_name;
                    char *key_type;
                    char *udf_suffix;

                    key_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
                    key_type = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
                    udf_suffix = "text";
                    if (!strcmp(key_type, "bigint"))
                    {
                        udf_suffix = "int";
                    }
                    else if (!strcmp(key_type, "double precision"))
                    {
                        udf_suffix = "float";
                    }
                    else if (!strcmp(key_type, "boolean"))
                    {
                        udf_suffix = "bool";
                    }
                    resetStringInfo(&buf);
                    appendStringInfo(&buf,
                                     "UPDATE %s SET %s = json_get_%s(json_data, '%s')",
                                     tname,
                                     key_name,
                                     udf_suffix,
                                     key_name);
                    ret = SPI_execute(buf.data, false, 0);
                    if (ret != SPI_OK_UPDATE)
                    {
                        elog(ERROR, "bw_colupgrade: column upgrade for '%s.%s' failed", tname, key_name);
                    }

                    /* Set dirty = false */
                    resetStringInfo(&buf);
                    appendStringInfo(&buf,
                                     "UPDATE %s.%s SET dirty = false WHERE "
                                     "key_name = '%s' AND key_type = '%s'",
                                     SCHEMA_NAME,
                                     tname,
                                     key_name,
                                     key_type);
                    ret = SPI_execute(buf.data, false, 0);
                    if (ret != SPI_OK_UPDATE || SPI_processed != 1)
                    {
                        elog(ERROR, "bw_colupgrade: could not update schema properly");
                    }
                    // FIXME: always ask for a lock on this table; if can't acquire, sleep

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
