#include <postgres.h>
#include <assert.h>

#include "json.h"
#include "utils.h"

char *
jsmntok_to_str(jsmntok_t *tok, char *json)
{
    char *retval;
    int   len;

    assert(tok && json);

    len = tok->end - tok->start;
    retval = pstrndup(&json[tok->start], len);

    return retval;
}

json_typeid
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
        if ((ptr = strchr(value_str, '.')) && !strchr(ptr + 1, '.')) // Only one decimal
        {
            return FLOAT;
        }
        else
        {
            return INTEGER;
        }
    default:
        elog(WARNING, "document: doc_insert: Got invalid json: %s", value_str);
        return NONE;
    }
}

json_typeid
jsmn_get_type(jsmntok_t* tok, char *json)
{
    jsmntype_t tok_type = tok->type;
    char *value_str = jsmntok_to_str(tok, json);

    if (!value_str) {
        return NONE;
    }

    switch (tok_type)
    {
    case JSMN_STRING:
        return STRING;
    case JSMN_PRIMITIVE:
        return jsmn_primitive_get_type(value_str);
    case JSMN_OBJECT:
        return DOCUMENT;
    case JSMN_ARRAY:
        return ARRAY;
    default:
        elog(WARNING, "document: Got invalid json: %s", value_str);
        return NONE;
    }
}


jsmntok_t *
jsmn_tokenize(char *json)
{
    jsmn_parser parser;
    jsmntok_t *tokens;
    unsigned maxToks;
    int status;

    jsmn_init(&parser);
    maxToks = 256;
    tokens = palloc0(sizeof(jsmntok_t) * maxToks);
    assert(tokens);

    if (json == NULL)
    {
        jsmntok_t *nulltok;

        elog(DEBUG5, "Null json");
        nulltok = palloc0(sizeof(json));
        nulltok->type = NONE;
        return nulltok;
    }
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
    elog(DEBUG5, "Completed parse");

    return tokens;
}

char *
get_pg_type(json_typeid type, char *value)
{
    json_typeid arr_elt_type;
    char *arr_elt_pg_type;
    char *buffer;
    jsmntok_t *tokens; /* In case we have an array and need to recur */

    assert(value);
    // elog(WARNING, "%d, %s", type, value);

    arr_elt_type = NONE;

    switch (type)
    {
        case STRING:
            return STRING_TYPE;
        case INTEGER:
            return INTEGER_TYPE;
        case FLOAT:
            return FLOAT_TYPE;
        case BOOLEAN:
            return BOOLEAN_TYPE;
        case DOCUMENT:
            return DOCUMENT_TYPE;
        case ARRAY:
            tokens = jsmn_tokenize(value);
            assert(tokens->type == JSMN_ARRAY);
            arr_elt_type = jsmn_get_type(tokens + 1, value);
            arr_elt_pg_type = get_pg_type(arr_elt_type, jsmntok_to_str(tokens + 1, value));
            buffer = palloc0(strlen(arr_elt_pg_type) + 2 + 1);
            sprintf(buffer, "%s%s", arr_elt_pg_type, ARRAY_TYPE);
            return buffer;
        case NONE:
            elog(WARNING, "document: got a null");
        default:
            elog(ERROR, "document: invalid type id on serialization");
    }
}

json_typeid
get_json_type(const char *pg_type)
{
    if (!strcmp(pg_type, STRING_TYPE))
    {
        return STRING;
    }
    else if (!strcmp(pg_type, INTEGER_TYPE))
    {
        return INTEGER;
    }
    else if (!strcmp(pg_type, FLOAT_TYPE))
    {
        return FLOAT;
    }
    else if (!strcmp(pg_type, BOOLEAN_TYPE))
    {
        return BOOLEAN;
    }
    else if (!strcmp(pg_type, DOCUMENT_TYPE))
    {
        return DOCUMENT;
    }
    else
    {
        int len;
        len = strlen(pg_type);
        if (len > 2 && pg_type[len-2] == '[' && pg_type[len-1] == ']')
        {
            return ARRAY;
        }
        else
        {
            elog(ERROR, "document: invalid type id on deserialization");
        }
    }
}
