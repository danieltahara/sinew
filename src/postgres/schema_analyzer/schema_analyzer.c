#include <postgres.h> /* This must precede all other includes */
#include <executor/spi.h>       /* this is what you need to work with SPI */
#include <commands/trigger.h>   /* ... and triggers */
#include <lib/stringinfo.h>
#include <utils/rel.h>

#include <assert.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define THRESHOLD_FREQUENCY (0.5)

static void update_key_counts(char *relname, char *doc, bool increment);

static void
update_key_counts(char *relname, char *doc, bool increment)
{
    int ret;
    int i, num_keys;
    StringInfoData buf;

    initStringInfo(&buf);

    num_keys = *(int*)doc;
    // elog(WARNING, "%d", num_keys);

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
                             "UPDATE document_schema.%s SET count = count + 1"
                             ", dirty = true WHERE key_id = %d",
                             relname,
                             id);
        }
        else
        {
            appendStringInfo(&buf,
                             "UPDATE document_schema.%s SET count = count - 1"
                             ", dirty = true WHERE key_id = %d",
                             relname,
                             id);
        }

        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_UPDATE)
        {
            elog(ERROR,
                 "analyze_document: SPI_execute failed (update count): error "
                 "code %d",
                 ret);
        }
        // elog(WARNING, "attempted update");

        /* Try to insert if key is new */
        if (SPI_processed != 1) {
            // elog(WARNING, "update failed; trying insert");
            if (!increment)
            {
                elog(ERROR,
                     "Key id (%d) not listed in attributes table for rel %s",
                     id,
                     relname);
            }
            resetStringInfo(&buf);
            appendStringInfo(&buf,
                             "INSERT INTO document_schema.%s (key_id, count, "
                             "dirty, upgraded) VALUES(%d, 1, 'true', 'false')",
                             relname,
                             id);
            ret = SPI_execute(buf.data, false, 0);
            if (ret != SPI_OK_INSERT || SPI_processed != 1)
            {
                elog(ERROR,
                     "analyze_document: SPI_execute failed (update count): "
                     "error code %d",
                     ret);
            }
            // elog(WARNING, "finished insert");
        }

        resetStringInfo(&buf);
    }
    // elog(WARNING, "end of analyze_doc");
}

Datum analyze_document(PG_FUNCTION_ARGS);
Datum analyze_schema(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(analyze_document);
PG_FUNCTION_INFO_V1(analyze_schema);

Datum
analyze_document(PG_FUNCTION_ARGS)
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
    bool isnull;

    if (!CALLED_AS_TRIGGER(fcinfo))
    {
        elog(ERROR, "analyze_document: not called by trigger manager");
    }

    if (SPI_connect() < 0)
    {
        elog(ERROR, "analyze_document: spi_connect failed");
    }

    tupdesc = trigdata->tg_relation->rd_att;
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
        TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) ||
        TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
    {
        int size;
        datum = (bytea*)DatumGetPointer(SPI_getbinval(trigdata->tg_trigtuple,
                                                      tupdesc,
                                                      2,
                                                      &isnull));
        size = VARSIZE(datum);
        // elog(WARNING, "size: %d", size - VARHDRSZ);

        assert(datum);
        doc_old = datum->vl_dat;
    }
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
    {
        datum = (bytea*)DatumGetPointer(SPI_getbinval(trigdata->tg_newtuple,
                                                      tupdesc,
                                                      2,
                                                      &isnull));
        assert(datum);
        doc_new = datum->vl_dat;
    }
    // elog(WARNING, "Got doc");

    rd_id = trigdata->tg_relation->rd_id;
    initStringInfo(&buf);
    appendStringInfo(&buf,
                     "SELECT relname FROM pg_class where oid = %d",
                     rd_id);

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "analyze_document: SPI_execute failed (get relname): error code"
            " %d", ret);
    }

    if (SPI_processed != 1) {
        PG_RETURN_NULL();
    }

    relname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
    {
        update_key_counts(relname, doc_old, true);
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
        elog(ERROR, "analyze_document: can only be called on insert/update/delete");
    }

    SPI_finish();
    return PointerGetDatum(rettuple);
}

/* TODO: trigger function for once/stmt evaluates the counts and updates
 * accordingly
 */
Datum
analyze_schema(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData*)fcinfo->context;
    Oid rd_id;
    int ret;
    StringInfoData buf;
    int count;
    char *relname;
    bool isnull;

    if (!CALLED_AS_TRIGGER(fcinfo))
    {
        elog(ERROR, "analyze_document: not called by trigger manager");
    }

    if (SPI_connect() < 0)
    {
        elog(ERROR, "analyze_document: spi_connect failed");
    }

    rd_id = trigdata->tg_relation->rd_id;
    // elog(WARNING, "got relation id: %d", rd_id);

    /* Get number of records in table */
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT n_live_tup FROM pg_stat_user_tables WHERE relid = %d", rd_id);
    // elog(WARNING, "%s", buf.data);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "analyze_document: SPI_execute failed (get record count): error code"
             " %d", ret);
    }
    if (SPI_processed != 1) {
        PG_RETURN_NULL();
    }
    // elog(WARNING, "HELLO");
    count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                        SPI_tuptable->tupdesc,
                                        1,
                                        &isnull));
    // elog(WARNING, "found %d tuples", count);

    /* Get relation name */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "SELECT relname FROM pg_class where oid = %d", rd_id);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "analyze_document: SPI_execute failed (get relname): error code"
             " %d", ret);
    }
    if (SPI_processed != 1) {
        PG_RETURN_NULL();
    }
    relname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    // elog(WARNING, "got relname: %s", relname);

    resetStringInfo(&buf);
    appendStringInfo(&buf,
                     "UPDATE document_schema.%s SET upgraded = 'true', "
                     "dirty = 'true' WHERE count >= %d AND upgraded = 'false'",
                     relname,
                     (int)(count * THRESHOLD_FREQUENCY));
    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_UPDATE)
    {
        elog(ERROR, "analyze_document: SPI_execute failed (upgrade cols): error code"
             " %d", ret);
    }
    // elog(WARNING, "upgraded");

    resetStringInfo(&buf);
    appendStringInfo(&buf,
                     "UPDATE document_schema.%s SET upgraded = 'false', "
                     "dirty = 'true' WHERE count < %d AND upgraded = 'true'",
                     relname,
                     (int)(count * THRESHOLD_FREQUENCY));
    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_UPDATE)
    {
        elog(ERROR, "analyze_document: SPI_execute failed (downgrade cols): error code"
             " %d", ret);
    }

    // elog(WARNING, "downgraded");

    SPI_finish();

    /* NOTE: This is weird; intuitively, it should be PG_RETURN_NULL() but that
     * was erroring out.
     * http://www.postgresql.org/docs/9.1/static/trigger-definition.html
     */
    return NULL;
}
