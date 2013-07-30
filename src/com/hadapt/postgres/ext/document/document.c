#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <funcapi.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/snapmgr.h>

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

#define STRING_TYPE "text"
#define INTEGER_TYPE "bigint"
#define FLOAT_TYPE "double precision"
#define BOOLEAN_TYPE "boolean"
#define DOCUMENT_TYPE "document"
#define ARRAY_TYPE "[]" /* The only non-terminal type we have */

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

static void
get_attribute(int id, char **key_ref, char **type_ref)
{
  // FIXME:
}

get_attribute_id(char *keyname, char *typename)
static int
{
    static char *last_keyname = NULL;
    static char *last_typename = NULL;
    static int last_attr_id = -1;

    StringInfoData buf;
    int ret;
    bool isnull;
    int attr_id;

    if (last_keyname && last_attr_id > 0 &&
        !strcmp(last_keyname, keyname) &&
        !strcmp(last_typename, typename)) {
        return last_attr_id;
    }

    SPI_connect();

    initStringInfo(&buf);
    appendStringInfo(&buf, "select _id from document_schema._attributes"
        " where key_name = '%s' AND key_type = '%s'", keyname, typename);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(ERROR, "document: SPI_execute failed: error code %d", ret);
    }

    if (SPI_processed != 1) {
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

    last_type = type;
    last_attr_id = attr_id;
    last_keyname = pstrndup(keyname, strlen(keyname)); /* Memory leak here */

    return attr_id;
}

static int
add_attribute(char *keyname, char *typename)
{
    SPI_connect();

    initStringInfo(&buf);
    appendStringInfo(&buf, "insert into document_schema._attributes values"
        "('%s', '%s')", keyname, typename);

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_INSERT)
    {
        elog(ERROR, "document: SPI_execute failed: error code %d", ret);
    }

    // FIXME: do i need transaction stuff?
    SPI_finish();

    return get_attribute_id(keyname, typename); /* Refreshes the cache and gives value */
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
            return document_to_binary(str, outbuff_ref);
        case ARRAY:
            return array_to_binary(str, outbuff_ref);
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
        const char *type;

        type = get_pg_type(doc.types[i], doc.values[i]);
        attr_ids[i] = get_attribute_id(doc.keys[i], type);
        if (attr_ids[i] < 0)
        {
            attr_ids[i] = add_attribute(doc.keys[i], type);
        }
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
Datum document_datum_to_string(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(string_to_document_datum);
PG_FUNCTION_INFO_V1(document_datum_to_string);

Datum
string_to_document_datum(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING_COPY(0);
    char *data;

    if (document_to_binary(str, &data) > 0)
    {
        PG_RETURN_BYTEA_P(data);
    }
    else
    {
        PG_RETURN_BYTEA_P(NULL); // FIXME: legal?
    }
}

static char *
binary_document_to_string(char *binary)
{
    document doc;
    int natts;
    char *result;
    int result_size, result_maxsize;

    binary_fill_document(binary, &doc);
    natts = doc->natts;

    result_size = 3; /* {\n} */
    result_maxsize = 64; // TODO: #define
    result = palloc0(result_maxsize + 1);
    strcat(result, "{\n");

    for (int i = 0; i < natts; i++)
    {
        char *key;
        char *value;
        char *attr;
        int attr_len;

        key = doc->keys[i];
        value = doc->values[i];

        attr_len = strlen(key) + strlen(value) + 5; /* "k":v,\n" */
        attr = palloc0(attr_len + 1);
        sprintf(result, "\"%s\":%s\n", key, value);

        if (result_size + attr_len + 1 >= result_maxsize)
        {
            result_maxsize = 2 * (result_size + attr_len) + 1;
            result = repalloc(result, result_maxsize + 1);
        }
        strcat(result, attr);
    }
    strcat(result, "}"); /* There is space because we keep adding an extra bit to result_maxsize */

    return result;
}

static void
binary_fill_document(char *binary, document *doc)
{
    int natts;
    int buffpos;
    char **keys;
    char **type_strings;
    json_typeid *types;
    char **values;

    assert(binary);

    memcpy(&natts, data, sizeof(int));
    buffpos = sizeof(int);

    keys = palloc0(natts * sizeof(char*));
    values = palloc0(natts * sizeof(char*));
    for (int i = 0; i < natts; i++)
    {
        int id;

        memcpy(&id, data + buffpos, sizeof(int));
        get_attribute(id, keys + i, type_strings +i);
        types[i] = get_json_type(type_strings[i]);
        buffpos += sizeof(int);
    }

    for (int i = 0; i < natts; i++)
    {
        int start, end;
        char *value_data;

        memcpy(&start, data + buffpos, sizeof(int));
        memcpy(&end, data + buffpos + sizeof(int), sizeof(int));

        value_data = palloc0(end - start);
        memcpy(value_data, data + start, end - start);
        values[i] = binary_to_string(types[i], value_data, end - start)

        pfree(value_data);

        buffpos += sizeof(int);
    }

    doc->natts = natts;
    doc->keys = keys;
    doc->types = types;
    doc->values = values;
}

static char *
binary_array_to_string(char *binary)
{
    int buffpos;
    int natts;
    json_typeid type;
    char *result;
    int result_size, result_maxsize;

    assert(binary);

    memcpy(&natts, binary, sizeof(int));
    memcpy(&type, binary + sizeof(int), sizeof(int));
    buffpos = 2 * sizeof(int);

    result_size = 2; /* '[]' */
    result_maxsize = 64
    result = palloc0(result_maxize + 1);
    strcat(result, "[");

    for (int i = 0; i < natts; i++) {
        int elt_size;
        char *elt;

        memcpy(&elt_size, binary + buffsize, sizeof(int));
        if (result_size + elt_size + 2 + 1 >= result_maxsize) {
            result_maxsize = 2 * (result_size + elt_size + 2) + 1;
            result = repalloc(result, result_maxsize + 1);
        }

        buffsize += sizeof(int);

        if (i != 0) {
           strcat(result, ", ");
        }

        elt = palloc0(elt_size);
        memcpy(elt, binary + buffsize, elt_size);
        strcat(result, binary_to_string(type, elt, elt_size);
        pfree(elt);

        buffsize += elt_size;
    }
    strcat(result, "]"); /* There is space because we keep adding an extra bit to result_maxsize */

    return result;
}

static char *
binary_to_string(json_typeid type, char *binary, int datum_len)
{
    char *result;
    int i;
    double d;

    assert(binary);

    result = palloc0(datum_len * 8 + 2 + 1); /* Guaranteed to be enough base 10 */

    switch (type)
    {
        case STRING:
            sprintf(result, "\"%s\"", binary);
            return result;
        case INTEGER:
            assert(datum_len == sizeof(int));
            memcpy(&i, binary, sizeof(int));
            sprintf(result, "%i", i);
            return result;
        case FLOAT:
            assert(datum_len == sizeof(double));
            memcpy(&d, binary, sizeof(double));
            sprintf(result, "%d", d);
            return result;
        case BOOLEAN:
            assert(datum_len == 1);
            sprintf(result, "%s", *binary != 0 ? "true" : "false");
            return result;
        case DOCUMENT:
            pfree(result);
            return binary_document_to_string(binary);
        case ARRAY:
            pfree(result);
            return binary_array_to_string(binary);
        case NONE:
        default:
            elog(ERROR, "document: invalid binary");
    }

}

const static char *
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

static char *
get_pg_type(json_typeid type, char *value)
{
    json_typeid arr_elt_type;
    char *arr_elt_pg_type;
    char *buffer;

    assert(value);

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
            memcpy(&arr_elt_type, value + sizeof(int), sizeof(int));
            arr_elt_pg_type = get_pg_type(arr_elt_typ, value + 2 * sizeof(int));
            buffer = palloc0(strlen(arr_elt_pg_type) + 2 + 1);
            sprintf(buffer, "%s%s", arr_elt_pg_type, ARRAY_TYPE);
            return buffer;
        default:
            elog(ERROR, "document: invalid type id on deserialization");
    }
}

Datum
document_datum_to_string(PG_FUNCTION_ARGS)
{
    char *data = PG_GETARG_BYTEA_P_COPY(0);
    // FIXME: need to check datum size
    // FIXME: this needs to go into serialization to create a varlena
    char *result;

    result = binary_document_to_string(data);

    return PG_RETURN_CSTRING(result);
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

/*******************************************************************************
 * Extraction Functions
 ******************************************************************************/

// For the next three can operate directly on binary data
Datum document_get(PG_FUNCTION_ARGS);
// // Datum document_put(PG_FUNCTION_ARGS);
// Datum document_delete(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(document_get);
// PG_FUNCTION_INFO_V1(document_put);
// PG_FUNCTION_INFO_V1(document_delete);

Datum
document_get(PG_FUNCTION_ARGS)
{
}
