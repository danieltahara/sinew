#include <postgres.h> /* This must precede all other includes */
#include "executor/spi.h"       /* this is what you need to work with SPI */
#include "commands/trigger.h"   /* ... and triggers */
#include "utils/stringinfo.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static void update_key_counts(char *relname, char *doc, bool increment);

Datum document_analyze(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(trigf);

Datum
document_analyze(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData*)fcinfo->context;
    TupleDesc   tupdesc;
    HeapTuple rettuple;
    Oid rd_id;
    int ret;
    StringInfoData buf;
    char *relname;
    bytea* datum;
    char *doc_old, *doc_new;

    if (!CALLED_AS_TRIGGER(fcinfo))
    {
        elog(ERROR, "document_analyze: not called by trigger manager");
    }

    tupdesc = trigdata->tg_tupdesc;
    datum = DatumGetPointer(SPI_getbinval(trigdata->tg_trigtuple,
                                          tupdesc,
                                          1,
                                          &isnull));
    doc_old = doc->vl_dat;
    datum = DatumGetPointer(SPI_getbinval(trigdata->tg_newtuple,
                                          tupdesc,
                                          1,
                                          &isnull));
    doc_new = doc->vl_dat;

    if (SPI_connect() < 0)
    {
        elog(ERROR, "document_analyze: spi_connect failed");
    }

    rd_id = trigdata->tg_relation->rd_id;
    initStringInfo(&buf);
    appendStringInfo("SELECT relname FROM pg_class where oid = %d", rd_id);

    ret = SPI_execute(buf, false, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "document_analyze: SPI_execute failed (get relname): error code"
            " %d", ret);
    }

    if (SPI_processed != 1) {
        PG_RETURN_NULL();
    }

    relname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
    {
        update_key_counts(relname, doc_new, true);
        rettuple = trigdata->tg_newtuple;
    }
    else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
    {
        update_key_counts(relname, doc_old, false);
        update_key_counts(relname, doc_new, true);
        rettuple = trigdata->tg_newtuple;
    }
    else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
    {
        update_key_counts(relname, doc_old, false);
        rettuple = trigdata->tg_trigtuple;
    }
    else
    {
        elog(ERROR, "document_analyze: can only be called on insert/update/delete");
    }

    SPI_finish();
    return rettuple;
}

static void
update_key_counts(char *relname, char *doc, bool increment)
{
    bool isnull;
    int ret;
    int i, num_keys;
    StringInfoData buf;

    initStringInfo(&buf);

    num_keys = *((int*)doc);

    for (i = 0; i < num_keys; ++i)
    {
        /* TODO: prepared statement
         * http://www.postgresql.org/docs/9.2/static/spi-spi-prepare.html
         */
        int id;

        id = *((int*)(doc + (i + 1) * sizeof(int)));

        /* Increment count of key appearances */
        if (increment)
        {
            appendStringInfo(&buf,
                             "UPDATE document_schema.%s SET count = count + 1 WHERE "
                             "key_id = %d",
                             relname,
                             id);
        }
        else
        {
            appendStringInfo(&buf,
                             "UPDATE document_schema.%s SET count = count - 1 WHERE "
                             "key_id = %d",
                             relname,
                             id);
        }

        ret = SPI_execute(buf, false, 0);
        if (ret != SPI_OK_UPDATE)
        {
            elog(ERROR,
                 "document_analyze: SPI_execute failed (update count): error "
                 "code %d",
                 ret);
        }

        /* Try to insert if key is new */
        if (SPI_processed != 1) {
            if (!increment)
            {
                elog(ERROR,
                     "Key id (%d) not listed in attributes table for rel %s",
                     id,
                     relname);
            }
            resetStringInfo(&buf);
            appendStringInfo(&buf,
                             "INSERT INTO document_schema.%s (key_id, count) "
                             "VALUES(%d, 1)",
                             relname,
                             id);
            ret = SPI_execute(buf, false, 0);
            if (ret != SPI_OK_INSERT || SPI_processed != 1)
            {
                elog(ERROR,
                     "document_analyze: SPI_execute failed (update count): "
                     "error code %d",
                     ret);
            }
        }

        resetStringInfo(&buf);
    }
}
