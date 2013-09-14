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

/*******************************************************************************
 * Document Schema Lookup
 ******************************************************************************/
static void
get_attribute(int id, char **key_name_ref, char **key_type_ref)
{
    StringInfoData buf;
    int ret;

    SPI_connect();

    initStringInfo(&buf);
    appendStringInfo(&buf, "select key_name, key_type from"
        " document_schema._attributes where _id = '%d'", id);
    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "document: SPI_execute failed (get_attribute): error code"
            " %d", ret);
    }

    if (SPI_processed != 1) {
        /* TODO: IF attribute doesn't exist, signal error */
        *key_name_ref = NULL;
        *key_type_ref = NULL;
        return;
    }

    *key_name_ref = SPI_getvalue(SPI_tuptable->vals[0],
                                 SPI_tuptable->tupdesc,
                                 1);
    *key_type_ref = SPI_getvalue(SPI_tuptable->vals[0],
                                 SPI_tuptable->tupdesc,
                                 2);
    assert(*key_name_ref && *key_type_ref);

    SPI_finish();
}

/* TODO: want to memoize this somehow; need to figure out right memory context
 */
static int
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
                       1, &isnull));
    if (isnull)
    {
        elog(ERROR, "document: null result");
    }

    SPI_finish();

    return attr_id;
}

static int
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

