/* -------------------------------------------------------------------------
 *
 * PostgreSQL Database Management System
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

 * https://github.com/postgres/postgres/tree/master/contrib/column_upgrade_bw
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
// #include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
// #include "lib/stringinfo.h"
// #include "pgstat.h"
// #include "utils/builtins.h"
// #include "utils/snapmgr.h"
// #include "tcop/utility.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(column_upgrade_bw_launch);

void		_PG_init(void);
void		column_upgrade_bw_main(Datum);
Datum		column_upgrade_bw_launch(PG_FUNCTION_ARGS);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	column_upgrade_bw_naptime = 180;
static int	column_upgrade_bw_total_workers = 1;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
column_upgrade_bw_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
column_upgrade_bw_sighup(SIGNAL_ARGS)
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
initialize_column_upgrade_bw(worktable *table)
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
column_upgrade_bw_main(Datum main_arg)
{
	char *dbName = DatumGetCString(main_arg);
	StringInfoData buf;
	char		name[20];

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, column_upgrade_bw_sighup);
	pqsignal(SIGTERM, column_upgrade_bw_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbName, NULL);

	elog(LOG, "%s initialized with %s.%s",
		 MyBgworkerEntry->bgw_name, table->schema, table->name);
	initialize_column_upgrade_bw(table);

// 	initStringInfo(&buf);
// 	appendStringInfo(&buf,
// 					 "WITH deleted AS (DELETE "
// 					 "FROM %s.%s "
// 					 "WHERE type = 'delta' RETURNING value), "
// 					 "total AS (SELECT coalesce(sum(value), 0) as sum "
// 					 "FROM deleted) "
// 					 "UPDATE %s.%s "
// 					 "SET value = %s.value + total.sum "
// 					 "FROM total WHERE type = 'total' "
// 					 "RETURNING %s.value",
// 					 table->schema, table->name,
// 					 table->schema, table->name,
// 					 table->name,
// 					 table->name);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			ret;
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   column_upgrade_bw_naptime * 1000L);
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

		/* We can now execute queries via SPI */
		ret = SPI_execute(buf.data, false, 0);

// TODO:
// Troll the schema
// sort by
// if we're under limit and there exist columns > schema
// if we add columns, reset to minimum
// else, double the time
// 		if (ret != SPI_OK_UPDATE_RETURNING)
// 			elog(FATAL, "cannot select from table %s.%s: error code %d",
// 				 table->schema, table->name, ret);
//
// 		if (SPI_processed > 0)
// 		{
// 			bool		isnull;
// 			int32		val;
//
// 			val = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
// 											  SPI_tuptable->tupdesc,
// 											  1, &isnull));
// 			if (!isnull)
// 				elog(LOG, "%s: count in %s.%s is now %d",
// 					 MyBgworkerEntry->bgw_name,
// 					 table->schema, table->name, val);
// 		}

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	proc_exit(1);
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
	DefineCustomIntVariable("column_upgrade_bw.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&column_upgrade_bw_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("column_upgrade_bw.total_workers",
							"Number of workers.",
							NULL,
							&column_upgrade_bw_total_workers,
							2,
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
	worker.bgw_main = column_upgrade_bw_main;
	worker.bgw_sighup = NULL;
	worker.bgw_sigterm = NULL;

	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
	for (i = 1; i <= column_upgrade_bw_total_workers; i++)
	{
		snprintf(worker.bgw_name, BGW_MAXLEN, "worker %d", i);
		worker.bgw_main_arg = Int32GetDatum(i);

		RegisterBackgroundWorker(&worker);
	}
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
column_upgrade_bw_launch(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0);
	BackgroundWorker worker;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;		/* new worker might not have library loaded */
	sprintf(worker.bgw_library_name, "column_upgrade_bw");
	sprintf(worker.bgw_function_name, "column_upgrade_bw_main");
	worker.bgw_sighup = NULL;	/* new worker might not have library loaded */
	worker.bgw_sigterm = NULL;	/* new worker might not have library loaded */
	snprintf(worker.bgw_name, BGW_MAXLEN, "worker %d", i);
	worker.bgw_main_arg = CStringGetDatum();

	PG_RETURN_BOOL(RegisterDynamicBackgroundWorker(&worker));
}