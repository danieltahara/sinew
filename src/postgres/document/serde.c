#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <funcapi.h>
#include <fmgr.h>

#include "document.h"
#include "utils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*******************************************************************************
 * De/Serialization
 ******************************************************************************/

Datum string_to_document_datum(PG_FUNCTION_ARGS);
Datum document_datum_to_string(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(string_to_document_datum);
PG_FUNCTION_INFO_V1(document_datum_to_string);

Datum
string_to_document_datum(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    char *binary;
    int size;
    bytea *datum;

    str = pstrndup(str, strlen(str));

    if ((size = document_to_binary(str, &binary)) > 0)
    {
        datum = palloc0(VARHDRSZ + size);
        SET_VARSIZE(datum, VARHDRSZ + size);
        memcpy(datum->vl_dat, binary, size); /* FIXME: Hack for now */
        PG_RETURN_POINTER(datum);
    }
    else
    {
        PG_RETURN_NULL();
    }
}

Datum
document_datum_to_string(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_POINTER(0);
    char *result;

    // TODO: I have the size sitting around here, could use it to verify
    result = binary_document_to_string(datum->vl_dat);

    PG_RETURN_CSTRING(result);
}
