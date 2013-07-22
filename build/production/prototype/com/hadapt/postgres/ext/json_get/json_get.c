#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include "lib/jsmn.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef enum { STRING = 1, INTEGER, FLOAT, BOOLEAN, NULL } json_typeid;

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
jsmn_get(jsmntok_t *tokens, char *key)
{
    jsmntok_t *retval = NULL;

    typedef enum { START, KEY, VALUE, STOP } parse_state;
    parse_state state = START;
    for (size_t i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok = &tokens[i];

        /* Should never reach uninitialized tokens */
        assert(t->start != -1 && t->end != -1);

        switch (state)
        {
        case START:
            assert(curtok->type == JSMN_OBJECT);
            j += t->size;
            state = KEY;
            break;
        case KEY:
            assert(curtok->type == JSMN_STRING);
            if (!strcmp(key, string_for_tok(curtok, json)))
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
            if (t->type == JSMN_ARRAY || t->type == JSMN_OBJECT)
            {
                i += t->size; /* Skip all of the tokens */
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
        switch(value_str[0]) {
        case 't': case 'f':
            return BOOLEAN;
        case 'n':
            return NULL;
        case '-':
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            char *ptr = NULL;
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
            return NULL;
        }
}

const static json_typeid
jsmn_get_type(jsmntok_type_t tok_type, char *value_str)
{
    if (!value_str) {
        return NULL;
    }

    switch (tok_type)
    {
    case JSMN_STRING:
        return STRING;
    case JSMN_ARRAY:
    case JSMN_OBJECT:
        return (strlen(value) == 0 ? NULL : STRING);
    case JSMN_PRIMITIVE:
        return jsmn_primitive_get_type(value_str);
    default:
        elog(WARNING, "doc_insert: Got invalid json: %s", value_str);
        return NULL;
    }
}

PG_FUNCTION_INFO_V1(json_get_int);
PG_FUNCTION_INFO_V1(json_get_float);
PG_FUNCTION_INFO_V1(json_get_bool);
PG_FUNCTION_INFO_V1(json_get_text);

typedef struct {
    json_type type;
    char *value;
} json_value;

json_value *
json_get_internal(char *json, char *key)
{
    jsmntok_t *tokens = json_tokenize(json);
    jsmntok_t *value_tok = jsmn_get(tokens, key);
    char *value_str = jsmntok_to_str(value_tok, json);

    json_value *retval = palloc0(sizeof(json_value));
    retval->value = value_str;
    retval->type = jsmn_get_type(value_tok-type, value_str);
    return retval;
}

Datum
json_get_int(PG_FUNCTION_ARGS)
{
    char *json = PG_GETARG_CSTRING(0);
    char *key = PG_GETARG_CSTRING(1);

    json = pstrdup(json);
    key = pstrdup(key);

    json_value *json_val = json_get_internal(json, key);
    json_type type = json_value->type;
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

    json = pstrdup(json);
    key = pstrdup(key);

    json_value *json_val = json_get_internal(json, key);
    json_type type = json_value->type;
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

    json = pstrdup(json);
    key = pstrdup(key);

    json_value *json_val = json_get_internal(json, key);
    json_type type = json_value->type;
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

    json = pstrdup(json);
    key = pstrdup(key);

    json_value *json_val = json_get_internal(json, key);
    json_type type = json_value->type;
    if (json_val->type == STRING)
    {
        PG_RETURN_CSTRING(json_val->value);
    }
    else
    {
        PG_RETURN_NULL();
    }
}
