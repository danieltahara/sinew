#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <funcapi.h>
#include <fmgr.h>

#include "lib/jsmn/jsmn.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef enum { STRING = 1,
               INTEGER,
               FLOAT,
               BOOLEAN,
               DOCUMENT,
               ARRAY,
               NONE
             } json_typeid;
               // ARRAY_S, /* Array of strings */
               // ARRAY_I, /* Array of ints */
               // ARRAY_F, /* Array of floats */
               // ARRAY_B, /* Array of booleans */
               // ARRAY_DOC /* Array of documents */
               // ARRAY_ARR /* Array of arrays; we are going to treat this as a
               //              string for the sake of avoiding deep recursion */

#define STRING_TYPE "text"
#define INTEGER_TYPE "bigint"
#define FLOAT_TYPE "double precision"
#define BOOLEAN_TYPE "boolean"
#define DOCUMENT_TYPE "document"
#define ARRAY_TYPE "[]"

static char *
pstrndup(char *str, int len)
{
    char *retval;

    retval = palloc0(len + 1);
    strncpy(retval, str, len);
    retval[len] = '\0';
    return retval;
}

static char *
jsmntok_to_str(jsmntok_t *tok, char *json)
{
    char *retval;
    int   len;

    assert(tok && json);

    len = tok->end - tok->start;
    retval = pstrndup(&json[tok->start], len);

    return retval;
}

static json_typeid
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
        elog(WARNING, "document: doc_insert: Got invalid json: %s", value_str);
        return NONE;
    }
}

static json_typeid
jsmn_get_type(jsmntok_t* tok, char *json)
{
    json_typeid tok_type = tok->type;
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

static int
get_attribute_id(char *keyname, json_typeid type)
{
    char *typename;
    //FIXME: lookup; add if necessary; add to top level transaction context
}

typedef struct {
    int        natts;
    char     **keys;
    json_type *types;
    char     **values;
} document;

static int
to_binary(json_type type, char *value, void **outbuff_ref)
{
    jsmntok_t *tokens;
    document doc;
    void *outbuff;

    outbuff = *outbuff_ref;

    switch (type)
    {
      // FIXME: convert to **
        case STRING:
            (char*)outbuff = pstrncpy(value, strlen(value));
            return strlen(value);
        case INTEGER:
            outbuff = palloc0(sizeof(int));
            *((int*)outbuff) = atoi(value);
            return sizeof(int);
        case FLOAT:
            outbuff = palloc0(sizeof(double));
            *((double*)outbuff) = atof(value);
            return sizeof(double);
            break;
        case BOOLEAN:
            outbuff = palloc0(1);
            if (!strcmp(value, "true")) {
                *(char*)outbuff = 1;
            } else if (!strcmp(value, "false")) {
                *(char*)outbuff = 0;
            } else {
                elog(WARNING, "document: boolean has invalid value");
                return -1;
            }
            return 1;
        case DOCUMENT:
            json_fill_document(str, &doc);
            return document_to_binary(&doc, outbuff);
        case ARRAY:
            return array_to_binary(str, outbuff);
        case NONE:
        case DEFAULT:
            elog(WARNING, "document: invalid data type");
            return -1;
    }
}

static int
document_to_binary(char *json, char **outbuff_ref)
{
    document doc;
    int natts;
    char *outbuff;

    json_fill_document(json, &doc);
    natts = doc.natts;
    outbuff = *outbuff_ref;

    int  attr_ids[natts];
    int *attr_id_refs[natts]; /* Just sort the pointer, so we can recover
                                     original position */
    for (int i = 0; i < natts; i++)
    {
        attr_ids[i] = get_attribute_id(doc.keys[i], doc.types[i]);
        attr_id_refs[i] = attr_ids + i;
    }
    qsort(attr_id_refs, natts, sizeof(int), intref_comparator);

    int data_size = 2 * natts * sizeof(int) + 1024;
    int buffpos = 0;
    outbuff = palloc0(data_size);
    memcpy(outbuff, &natts, sizeof(int));
    buffpos += sizeof(int);
    for (int i = 0; i < natts; i++)
    {
        memcpy(outbuff + buffpos, *(attr_id_refs + i), sizeof(int));
        buffpos += sizeof(int);
    }
    /* Copy data and offsets */
    buffpos = sizeof(int) + 2 * sizeof(int) * natts +
        sizeof(int); /* # attrs, attr_ids, offsets, length */
    for (int i = 0; i < natts; i++)
    {
        char *binary;
        int *attr_id_ref = attr_id_refs[i];
        int orig_pos = attr_id_ref - attr_ids;
        int datum_size = to_binary(doc.types[orig_pos], doc.values[orig_pos],
            binary);
        memcpy(outbuff + buffpos, binary, datum_size);
        memcpy(outbuff + (1 + natts + i) * sizeof(int), &buffpos,
            sizeof(int);
        buffpos += datum_size;
        if (buffpos >= data_size)
        {
            data_size = 2 * buffpos + 1;
            outbuff = repalloc(outbuff, data_size);
        }

        /* Cleanup */
        pfree(binary);
    }
    memcpy(outbuff + (1 + 2 * natts) * sizeof(int), &buffpos, sizeof(int));

    return buffpos;
}

static int
array_to_binary(char *json_arr, char **outbuff_ref)
{
    jsmntok_t *tokens;
    int data_size;
    int buffpos;
    int arrlen;
    json_typeid arrtype;
    char *outbuff;

    tokens = jsmn_tokenize(str);
    assert(tokens->type == JSMN_ARRAY);
    outbuff = *outbuff_ref;

    arrlen = tokens->size;
    arrtype = jsmn_get_type(curtok, json_arr);
    assert(arrtype != DOCUMENT); // TODO: for now

    data_size = 2 * natts * sizeof(int) + 1024;
    outbuff = palloc0(data_size);
    buffpos = 0;

    memcpy(outbuff + buffpos, &arrlen, sizeof(int));
    buffpos += sizeof(int);
    memcpy(outbuff + buffpos, &arrtype, sizeof(int));
    buffpos += sizeof(int);

    jsmntok_t *curtok;
    curtok = tokens + 1;
    for (int i = 0; i < arrlen; i++)
    {
        char *binary;
        int datum_size;

        if (jsmn_get_type(curtok, json_arr) != arrtype)
        {
            elog(WARNING, "document: inhomogenous types in JSMN_ARRAY: %s", json_arr);
            pfree(outbuff);
            outbuff = NULL;
            return 0;
        }

        datum_size = to_binary(arrtype, jsmntok_to_str(curtok, json_arr), binary);
        memcpy(outbuff + buffpos, &datum_size, sizeof(int));
        buffpos += sizeof(int);
        memcpy(outbuff + buffpos, binary, datum_size);
        buffpos += datum_size;

        if (buffpos >= data_size)
        {
            data_size = 2 * buffpos + 1;
            outbuff = repalloc(outbuff, data_size);
        }

        pfree(binary);
        if (curtok->type == JSMN_ARRAY || curtok->type == JSMN_ARRAY) {
           curtok += curtok->size;
        }
        ++curtok;
    }

    pfree(tokens);

    return buffpos;
}


Datum string_to_document_datum(PG_FUNCTION_ARGS);
//Datum document_datum_to_string(PG_FUNCTION_ARGS);

// For the next three can operate directly on binary data
// Datum document_get(PG_FUNCTION_ARGS);
// // Datum document_put(PG_FUNCTION_ARGS);
// Datum document_delete(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(string_to_document_datum);
//PG_FUNCTION_INFO_V1(document_datum_to_string);

PG_FUNCTION_INFO_V1(document_get);
PG_FUNCTION_INFO_V1(document_put);
PG_FUNCTION_INFO_V1(document_delete);

static Datum
string_to_document_datum(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING_COPY(0);
    char *data;

    // FIXME: load catalog into top transactioncontext
    if (document_to_binary(str, data) > 0)
    {
        return PointerGetDatum(data);
    }
    else
    {
        return PointerGetDatum(NULL); // TODO: valid?
    }
}

static void
json_fill_document(char *json, document *doc)
{
    int natts;
    int capacity;
    jsmntok_t *tokens;

    assert(json);
    assert(doc);

    tokens = jsmn_tokenize(json);
    natts = 0;

    size_t i, j;

    typedef enum { START, KEY, VALUE } parse_state;
    parse_state state;

    state = START;
    for (i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok;
        char *keyname;
        char *value;
        json_typeid type;
        int bytes;

        curtok = tokens + i;
        type = NONE;

        /* Should never reach uninitialized tokens */
        assert(curtok->start != -1 && curtok->end != -1);

        switch (state)
        {
        case START:
            assert(curtok->type == JSMN_OBJECT);

            capacity = curtok->size;
            doc->keys = palloc0(capacity * sizeof(char*));
            doc->types = palloc0(capacity * sizeof(int));
            doc->values = palloc0(capacity * sizeof(char*));

            j += capacity;
            state = KEY;
            break;
        case KEY:
            keyname = jsmntok_to_str(curtok, json);
            state = VALUE;
            break;
        case VALUE:
            if (curtok->type == JSMN_ARRAY)
            {
                i += curtok->size; /* Skip all of the tokens */
            }
            else if (curtok->type == JSMN_OBJECT)
            {
                i += curtok->size; /* Skip all of the tokens */
            }
            value = jsmntok_to_str(curtok, json);
            type = jsmn_get_type(curtok, json);

            doc->keys[natts] = keyname;
            doc->types[natts] = type;
            doc->values[natts] = value;

            ++natts;
            if (natts > capacity) {
                capacity = 2 * natts;
                doc->keys = repalloc(capacity * sizeof(char*));
                doc->types = repalloc(capacity * sizeof(int));
                doc->values = repalloc(capacity * sizeof(char*));
            }

            state = KEY;
            break;
        }
    }
    pfree(tokens)
    return retval;
}

static jsmntok_t *
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
        elog(DEBUG5, "Null json");
        retval = palloc0(sizeof(json_value));
        retval->type = NONE;
        return retval;
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
