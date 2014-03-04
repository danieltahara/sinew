#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <fmgr.h>
#include <funcapi.h>

#include <assert.h>

#include "document.h"
#include "schema.h"
#include "utils.h"

/*******************************************************************************
 * Extraction Functions
 ******************************************************************************/

/* NOTE: Because format is known, methods can operate directly on binary data */
/* TODO: should honestly make the _get methods take const char* */
Datum json_put(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(json_put);

Datum
json_put(PG_FUNCTION_ARGS)
{
    char *datum = (char*)PG_GETARG_CSTRING(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    char *attr_value = PG_GETARG_CSTRING(2);
    char *data;
    int size;
    bytea* outdatum;
    jsmntok_t *tokens, *curtok;
    int natts, start, end;
    char *key;

    data = palloc0(10000);
    tokens = jsmn_tokenize(datum);
    curtok = tokens;
    natts = curtok->size;

    ++curtok;
    while (curtok - tokens < natts) {
        key = jsmntok_to_str(curtok, datum);
        if (!strcmp(key, attr_path)) {
            ++curtok;
            start = curtok->start;
            end = curtok->end;
            break;
        }

        // To value
        ++curtok;
        if (curtok->type == JSMN_ARRAY) {
            curtok += curtok->size;
        }
        if (curtok->type == JSMN_OBJECT) {
            curtok += curtok->size;
        }

        pfree(key);
    }

    strncpy(data, datum, start);
    strcat(data, attr_value);
    strcat(data, datum + end);

    outdatum = palloc0(VARHDRSZ + strlen(data));
    SET_VARSIZE(outdatum, VARHDRSZ + strlen(data));
    memcpy(outdatum->vl_dat, data, strlen(data));
    PG_RETURN_POINTER(outdatum);
}


-- Put JSON

CREATE OR REPLACE FUNCTION
json_put(cstring, cstring, cstring)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;
