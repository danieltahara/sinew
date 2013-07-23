#include <postgres.h> /* This include must precede all other postgres
                         dependencies */
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <funcapi.h>

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include "lib/jsmn/jsmn.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef enum { STRING = 1, INTEGER, FLOAT, BOOLEAN, NONE } json_typeid;

static char *
jsmntok_to_str(jsmntok_t *tok, char *json)
{
    char *retval;
    int   len;

    len = tok->end - tok->start;
    retval = palloc0(len + 1);
    strncpy(retval, json, len);

    return retval;
}

// TODO: JSMN_example license
// http://alisdair.mcdiarmid.org/2012/08/14/jsmn-example.html
static jsmntok_t *
jsmn_get(jsmntok_t *tokens, char *json, char *key)
{
    jsmntok_t *retval = NULL;
    size_t i, j;

    typedef enum { START, KEY, VALUE, STOP } parse_state;
    parse_state state = START;
    for (i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok = &tokens[i];

        /* Should never reach uninitialized tokens */
        assert(curtok->start != -1 && curtok->end != -1);

        switch (state)
        {
        case START:
            assert(curtok->type == JSMN_OBJECT);
            j += curtok->size;
            state = KEY;
            break;
        case KEY:
            assert(curtok->type == JSMN_STRING);
            if (!strcmp(key, jsmntok_to_str(curtok, json)))
            {
                assert(j >= 1);
                retval = curtok + 1;
                state = STOP;
            }
            else
            {
                state = VALUE;
            }
            break;
        case VALUE:
            if (curtok->type == JSMN_ARRAY || curtok->type == JSMN_OBJECT)
            {
                i += curtok->size; /* Skip all of the tokens */
            }
            state = KEY;
            break;
        case STOP:
            return retval;
        }
    }
    return retval;
}

const static json_typeid
jsmn_primitive_get_type(char *value_str)
{
        char *ptr;
        switch(value_str[0]) {
        case 't': case 'f':
            return BOOLEAN;
        case 'n':
            return NONE;
        case '-':
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            ptr = NULL;
            if ((ptr = strchr(value_str, '.')) && !strchr(ptr, '.')) // Only one decimal
            {
                return FLOAT;
            }
            else
            {
                return INTEGER;
            }
        default:
            elog(WARNING, "doc_insert: Got invalid json: %s", value_str);
            return NONE;
        }
}

const static json_typeid
jsmn_get_type(jsmntype_t tok_type, char *value_str)
{
    if (!value_str) {
        return NONE;
    }

    switch (tok_type)
    {
    case JSMN_STRING:
        return STRING;
    case JSMN_ARRAY:
    case JSMN_OBJECT:
        return (strlen(value_str) == 0 ? NONE : STRING);
    case JSMN_PRIMITIVE:
        return jsmn_primitive_get_type(value_str);
    default:
        elog(WARNING, "doc_insert: Got invalid json: %s", value_str);
        return NONE;
    }
}

/********************************************************************************
* Main UDF Definitions
********************************************************************************/

typedef struct {
    json_typeid type;
    char *value;
} json_value;

Datum json_get_int(PG_FUNCTION_ARGS);
Datum json_get_float(PG_FUNCTION_ARGS);
Datum json_get_bool(PG_FUNCTION_ARGS);
Datum json_get_text(PG_FUNCTION_ARGS);
json_value *json_get_internal(char *json, char *key);

PG_FUNCTION_INFO_V1(json_get_int);
PG_FUNCTION_INFO_V1(json_get_float);
PG_FUNCTION_INFO_V1(json_get_bool);
PG_FUNCTION_INFO_V1(json_get_text);

json_value *
json_get_internal(char *json, char *key)
{
    jsmn_parser parser;
    jsmntok_t *tokens;
    unsigned maxToks;
    int status;
    jsmntok_t *value_tok;
    char *value_str;
    json_value *retval;

    jsmn_init(&parser);
    maxToks = 256;
    tokens = palloc0(sizeof(jsmntok_t) * maxToks);
    assert(tokens);

    status = jsmn_parse(&parser, json, tokens, maxToks);
    while (status == JSMN_ERROR_NOMEM)
    {
        maxToks = maxToks * 2 + 1;
        tokens = repalloc(tokens, sizeof(jsmntok_t) * maxToks);
        assert(tokens);
        status = jsmn_parse(&parser, json, tokens, maxToks);
    }

    if (status == JSMN_ERROR_INVAL)
    {
        elog(ERROR, "json_get: jsmn: invalid JSON string");
    }

    if (status == JSMN_ERROR_PART)
    {
        elog(ERROR, "json_get: jsmn: truncated JSON string");

    }

    /* Extract value from tokens */
    value_tok = jsmn_get(tokens, json, key);
    value_str = jsmntok_to_str(value_tok, json);
    retval = palloc0(sizeof(json_value));
    retval->value = value_str;
    retval->type = jsmn_get_type(value_tok->type, value_str);

    /* Cleanup */
    pfree(tokens);

    return retval;
}

Datum
json_get_int(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    char *key = PG_GETARG_CSTRING(1);
    json_value *json_val;

    json = pstrdup(json);
    key = pstrdup(key);

    json_val = json_get_internal(json, key);
    if (json_val->type == INTEGER)
    {
        PG_RETURN_INT64(atoi(json_val->value));
    }
    else
    {
        PG_RETURN_NULL();
    }
}

Datum
json_get_bool(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    char *key = PG_GETARG_CSTRING(1);
    json_value *json_val;

    json = pstrdup(json);
    key = pstrdup(key);

    json_val = json_get_internal(json, key);
    if (json_val->type == BOOLEAN)
    {
        PG_RETURN_BOOL(!strcmp(json_val->value, "true"));
    }
    else
    {
        PG_RETURN_NULL();
    }
}

Datum
json_get_float(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    char *key = PG_GETARG_CSTRING(1);
    json_value *json_val;

    json = pstrdup(json);
    key = pstrdup(key);

    json_val = json_get_internal(json, key);
    if (json_val->type == FLOAT)
    {
        PG_RETURN_FLOAT8(atol(json_val->value));
    }
    else
    {
        PG_RETURN_NULL();
    }
}

Datum
json_get_text(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    char *key = PG_GETARG_CSTRING(1);
    json_value *json_val;

    json = pstrdup(json);
    key = pstrdup(key);

    json_val = json_get_internal(json, key);
    if (json_val->type == STRING)
    {
        PG_RETURN_CSTRING(json_val->value);
    }
    else
    {
        PG_RETURN_NULL();
    }
}
