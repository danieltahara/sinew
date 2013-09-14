#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>

#include "lib/jsmn/jsmn.h"
#include "utils.h"
#include "json.h"
#include "schema.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Globals */
static TransactionId xid = 0;
static int num_keys = 0; /* Length of key_names and key_types */
static char **key_names = NULL;
static char **key_types = NULL;

/* NOTE: Assumes that key_names, key_types are valid */
static void
get_attr_info(int id, char **key_name_ref, char **key_type_ref)
{
    if (id > num_keys)
    {
         *key_name_ref = NULL;
         *key_type_ref = NULL;
    }
    else
    {
         *key_name_ref = key_names[id];
         *key_type_ref = key_types[id];
    }
}

/*******************************************************************************
 * Document Schema Lookup
 ******************************************************************************/

void
get_attribute(int id, char **key_name_ref, char **key_type_ref)
{
     StringInfoData buf;
     int ret;
 
     if (xid != GetCurrentTransactionId() || !key_names)
     {
         void *(*xact_palloc0)(MemoryContext, Size);
         bool isnull;
 
         xid = GetCurrentTransactionId();
 
         SPI_connect();
 
         initStringInfo(&buf);
         appendStringInfo(&buf,
                          "select id, key_name, key_type from "
                          "document_schema._attributes SORT BY id ASCENDING"); // Is Ascending tag necessary?
         ret = SPI_execute(buf.data, false, 0);
         if (ret != SPI_OK_SELECT)
         {
             elog(ERROR,
                  "document: SPI_execute failed (get_attribute): error code %d"
                  ret);
         }
 
         xact_palloc0 = MemoryContextAllocZero;
         num_keys = DatumGetInt32(SPI_getbinval(
               SPI_tuptable->vals[SPI_processed - 1],
               SPI_tuptable->tupdesc,
               1,
               &isnull));
         assert(!isnull);
 
         /* Memory was already freed by MemoryContext stuff, so I don't have to
          * redo it
          */
         key_names = xact_palloc0(CurTransactionContext,
                                  num_keys * sizeof(char*));
         key_types = xact_palloc0(CurTransactionContext,
                                   num_keys * sizeof(char*));
 
         for (i = 0; i < SPI_processed; ++i)
         {
             int aid;
             char *name, *val;
 
             aid = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                               SPI_tuptable->tupdesc,
                                               1,
                                               &isnull);
             assert(!isnull);
             name = SPI_getvalue(SPI_tuptable->vals[i],
                                 SPI_tuptable->tupdesc,
                                 1);
             key_names[aid] = xact_palloc0(CurTransactionContext,
                                           strlen(name) + 1);
             strcpy(key_names[aid], name);
             val = SPI_getvalue(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc,
                                2);
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
                  "document: SPI_execute failed (get_attribute): error code %d"
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
    if (isnull)
    {
        elog(ERROR, "document: null result");
    }

    SPI_finish();

    return attr_id;
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

    // FIXME: do i need transaction stuff?
    SPI_finish();

    return get_attribute_id(keyname, typename); /* Refreshes the cache and gives value */
}

