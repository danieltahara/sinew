#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <access/xact.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/memutils.h>

#include <assert.h>

#include "lib/jsmn/jsmn.h"
#include "utils.h"
#include "json.h"
#include "hash_table.h"
#include "schema.h"

/* Globals */
static TransactionId info_xid = 0;
static int num_keys = 0; /* Length of key_names and key_types */
static char **key_names = NULL;
static char **key_types = NULL;

static TransactionId attr_xid = 0;
static table_t *attr_table = NULL;

/* TODO: at some point, I might want to make the caching on key by key basis
 * rather than global */

/* NOTE: Assumes that key_names, key_types are valid */
static void
get_attr_info(int id, char **key_name_ref, char **key_type_ref)
{
    // elog(WARNING, "get attr info: %d", id);
    if (id > num_keys)
    {
         *key_name_ref = NULL;
         *key_type_ref = NULL;
    }
    else
    {
         *key_name_ref = pstrndup(key_names[id], strlen(key_names[id]));
         *key_type_ref = pstrndup(key_types[id], strlen(key_types[id]));
    }
    // elog(WARNING, "finished");
}

/*******************************************************************************
 * Document Schema Lookup
 ******************************************************************************/

void
get_attribute(int id, char **key_name_ref, char **key_type_ref)
{
    StringInfoData buf;
    int ret;

    if (info_xid != GetCurrentTransactionId() || !key_names)
    {
        void *(*xact_palloc0)(MemoryContext, Size);
        bool isnull;
        int i;

        info_xid = GetCurrentTransactionId();

        SPI_connect();

        initStringInfo(&buf);
        appendStringInfo(&buf,
                         "select _id, key_name, key_type from "
                         "document_schema._attributes ORDER BY _id ASC"); // TODO: Is Ascending tag necessary?
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT)
        {
            elog(ERROR,
                 "document: SPI_execute failed (get_attribute): error code %d",
                 ret);
        }

        xact_palloc0 = MemoryContextAllocZero;
        num_keys = DatumGetInt32(SPI_getbinval(
              SPI_tuptable->vals[SPI_processed - 1],
              SPI_tuptable->tupdesc,
              1,
              &isnull));
        assert(!isnull);
        // elog(WARNING, "num keys: %d", num_keys);

        /* Memory was already freed by MemoryContext stuff, so I don't have to
         * redo it
         */
        key_names = xact_palloc0(CurTransactionContext,
                                 num_keys * sizeof(char*));
        key_types = xact_palloc0(CurTransactionContext,
                                  num_keys * sizeof(char*));

        // elog(WARNING, "allocated key types and names");
        // elog(WARNING, "SPI_processed: %d", SPI_processed);
        for (i = 0; i < SPI_processed; ++i)
        {
            int aid;
            char *name, *val;

            aid = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                              SPI_tuptable->tupdesc,
                                              1,
                                              &isnull));
            // elog(WARNING, "found aid: %d", aid);
            assert(!isnull);
            name = SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc,
                                2);
            key_names[aid] = xact_palloc0(CurTransactionContext,
                                          strlen(name) + 1);
            strcpy(key_names[aid], name);
            val = SPI_getvalue(SPI_tuptable->vals[i],
                               SPI_tuptable->tupdesc,
                               3);
            key_types[aid] = xact_palloc0(CurTransactionContext,
                                          strlen(val) + 1);
            strcpy(key_types[aid], val);
        }

        SPI_finish();

        get_attr_info(id, key_name_ref, key_type_ref);
        return;
    }
    else if (id > num_keys || !key_names[id])
    {
        /* Cache miss.
         * get_attribute only occurs on deserialization, so this is unlikely to
         * be called, and hence we don't mind paying the cost of single lookup
         */
        SPI_connect();

        initStringInfo(&buf);
        appendStringInfo(&buf, "select key_name, key_type from"
        " document_schema._attributes where _id = '%d'", id);

        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT)
        {
            elog(ERROR,
                 "document: SPI_execute failed (get_attribute): error code %d",
                 ret);
        }

        if (SPI_processed != 1)
        {
            *key_name_ref = NULL;
            *key_type_ref = NULL;
        }
        else
        {
            *key_name_ref = SPI_getvalue(SPI_tuptable->vals[0],
                                         SPI_tuptable->tupdesc,
                                         1);
            *key_type_ref = SPI_getvalue(SPI_tuptable->vals[0],
                                         SPI_tuptable->tupdesc,
                                         2);
        }
        return;
    }
    else
    {
        get_attr_info(id, key_name_ref, key_type_ref);
        return;
    }
}

int
get_attribute_id(const char *keyname, const char *typename)
{
    StringInfoData buf;
    int ret;
    bool isnull;
    int attr_id;
    char *attr;

    attr = palloc0(strlen(keyname) + strlen(typename) + 2);
    sprintf(attr, "%s %s", keyname, typename);
    // elog(WARNING, "Looking for attr: %s", attr);

    if (attr_xid != GetCurrentTransactionId() || !attr_table)
    {
        bool isnull;
        int i;

        attr_xid = GetCurrentTransactionId();

        SPI_connect();

        initStringInfo(&buf);
        appendStringInfo(&buf,
                         "select _id, key_name, key_type from "
                         "document_schema._attributes");
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT)
        {
            elog(ERROR,
                 "document: SPI_execute failed (get_attribute): error code %d",
                 ret);
        }

        /* Memory was already freed by MemoryContext stuff, so I don't have to
         * redo it
         */
        attr_table = make_table();

        for (i = 0; i < SPI_processed; ++i)
        {
            int aid;
            char *name, *type;
            char *attr_tmp;

            aid = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                              SPI_tuptable->tupdesc,
                                              1,
                                              &isnull));
            assert(!isnull);
            name = SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc,
                                2);
            type = SPI_getvalue(SPI_tuptable->vals[i],
                               SPI_tuptable->tupdesc,
                               3);
            attr_tmp = palloc0(strlen(name) + strlen(type) + 2);
            sprintf(attr_tmp, "%s %s", name, type);
            // elog(WARNING, "caching value: %s", attr_tmp);

            put(attr_table, attr_tmp, aid);
            pfree(attr_tmp);
        }

        SPI_finish();

        return get(attr_table, attr);
    }
    else if ((attr_id = get(attr_table, attr)) >= 0)
    {
        // elog(WARNING, "found attr id in table");
        return attr_id;
    }
    else
    {
        SPI_connect();

        initStringInfo(&buf);
        appendStringInfo(&buf, "select _id from document_schema._attributes"
            " where key_name = '%s' AND key_type = '%s'", keyname, typename);
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT)
        {
            elog(ERROR, "document: SPI_execute failed: error code %d", ret);
        }

        if (SPI_processed != 1) {
            SPI_finish();
            return -1;
        }

        attr_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                              SPI_tuptable->tupdesc,
                                              1,
                                              &isnull));
        assert(!isnull);
        SPI_finish();

        /* Update table */
        attr = palloc0(strlen(keyname) + strlen(typename) + 2);
        sprintf(attr, "%s %s", keyname, typename);
        put(attr_table, attr, attr_id);

        return attr_id;
    }
}

int
add_attribute(const char *keyname, const char *typename)
{
    int ret; /* Return code of SPI_execute */
    StringInfoData buf;

    SPI_connect();

    initStringInfo(&buf);
    appendStringInfo(&buf, "insert into document_schema._attributes(key_name, "
        "key_type) values ('%s', '%s')", keyname, typename);

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_INSERT)
    {
        elog(ERROR, "document: SPI_execute failed: error code %d", ret);
    }

    SPI_finish();

    return get_attribute_id(keyname, typename); /* Refreshes the cache and gives value */
}

