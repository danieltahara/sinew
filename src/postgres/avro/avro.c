#include <postgres.h> /* This include must precede all other postgres
dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/snapmgr.h>
#include <utils/array.h>

#include "lib/jsmn/jsmn.h"
#include "document.h"
#include "schema.h"
#include "utils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum string_to_avro_datum(PG_FUNCTION_ARGS);
Datum avro_datum_to_string(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(string_to_avro_datum);
PG_FUNCTION_INFO_V1(avro_datum_to_string);

Datum
string_to_avro_datum(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    char *schema = PG_GETARG_CSTRING(1);

    // char *binary;
    // int size;
    // bytea *datum;

    // str = pstrndup(str, strlen(str));

    // if ((size = document_to_binary(str, &binary)) > 0)
    // {
    //     datum = palloc0(VARHDRSZ + size);
    //     SET_VARSIZE(datum, VARHDRSZ + size);
    //     memcpy(datum->vl_dat, binary, size); /* FIXME: Hack for now */
    //     PG_RETURN_POINTER(datum);
    // }
    // else
    // {
    //     PG_RETURN_NULL();
    // }
}

Datum
avro_datum_to_string(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_POINTER(0);
    char *schema_str = PG_GETARG_CSTRING(1);

    avro_value_iface_t *iface;
    avro_value_t value;
    char *result;

    avro_schema_t schema;
    if (avro_schema_from_json_literal(schema_str, &schema)) {
        // fprintf(stderr, "Unable to parse person schema\n");
        // TODO: Error
    }
    iface = avro_generic_class_from_schema(schema);

    value.iface = iface;
    value.self = datum;
    avro_value_to_json(value, 1, &result);

    PG_RETURN_CSTRING(result);
}
