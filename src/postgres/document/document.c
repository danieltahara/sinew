#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <funcapi.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/snapmgr.h>
#include <utils/array.h>

#include "lib/jsmn/jsmn.h"
#include "utils.h"
#include "json.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*******************************************************************************
 * Document Schema Lookup
 ******************************************************************************/

static void get_attribute(int id,
                          char **key_name_ref,
                          char **type_name_ref); /* TODO: better name */
static int get_attribute_id(const char *key_name, const char *type_name);
static int add_attribute(const char *key_name, const char *type_name);

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

/*******************************************************************************
 * De/Serialization
 ******************************************************************************/

typedef struct {
    int        natts;
    char     **keys;
    json_typeid *types;
    char     **values;
} document;

static void json_to_document(char *json, document *doc);
static int array_to_binary(char *json_arr, char **outbuff_ref);
static int document_to_binary(char *json, char **outbuff_ref);
static int to_binary(json_typeid type, char *value, char **outbuff_ref);

/* TODO: should probably pass an outbuff ref in the same way as above */
/* NOTE: the reason I pass an outbuff ref above is because the document binary
 * can potentially have an x00 value, which would mess up attempts at using
 * strlen(str). Since the deserialization functions return json strings, they
 * do not face the same problem */
static void binary_to_document(char *binary, document *doc);
static char *binary_document_to_string(char *binary);
static char *binary_array_to_string(char *binary);
static char *binary_to_string(json_typeid type, char *binary, int datum_len);

Datum string_to_document_datum(PG_FUNCTION_ARGS);
Datum document_datum_to_string(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(string_to_document_datum);
PG_FUNCTION_INFO_V1(document_datum_to_string);

static void
json_to_document(char *json, document *doc)
{
    int natts;
    int capacity;
    jsmntok_t *tokens;
    size_t i, j;
    typedef enum { START, KEY, VALUE } parse_state;
    parse_state state;

    assert(json);
    assert(doc);

    tokens = jsmn_tokenize(json);
    natts = 0;

    state = START;
    for (i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok;
        char *keyname;
        char *value;
        json_typeid type;

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
            value = jsmntok_to_str(curtok, json);
            type = jsmn_get_type(curtok, json);

            if (curtok->type == JSMN_ARRAY)
            {
                jsmntok_t *tok;
                int end;

                tok = curtok;
                end = curtok->end;
                while (tok->start <= end) {
                    ++i;
                    tok = tokens + i;
                }
                --i; /* Hack because loop increments i for us */
                // TODO: get rid of i in loop; just increment curtok
                // properly
            }
            else if (curtok->type == JSMN_OBJECT)
            {
                // FIXME:
                i += curtok->size; /* Skip all of the tokens */
            }

            doc->keys[natts] = keyname;
            doc->types[natts] = type;
            doc->values[natts] = value;
            elog(WARNING, "%s, %d, %s", keyname, type, value);

            ++natts;
            if (natts > capacity) {
                capacity = 2 * natts;
                doc->keys = repalloc(doc->keys, capacity * sizeof(char*));
                doc->types = repalloc(doc->types, capacity * sizeof(int));
                doc->values = repalloc(doc->values, capacity * sizeof(char*));
            }

            state = KEY;
            break;
        }
    }
    pfree(tokens);
    doc->natts = natts;
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
    jsmntok_t *curtok; /* Current token being processed */
    int i; /* Loop variable */

    tokens = jsmn_tokenize(json_arr);
    assert(tokens->type == JSMN_ARRAY);
    outbuff = *outbuff_ref;

    arrlen = tokens->size;
    arrtype = jsmn_get_type(tokens + 1, json_arr);

    data_size = 2 * arrlen * sizeof(int) + 1024;
    outbuff = palloc0(data_size);
    buffpos = 0;

    memcpy(outbuff + buffpos, &arrlen, sizeof(int));
    buffpos += sizeof(int);
    memcpy(outbuff + buffpos, &arrtype, sizeof(int));
    buffpos += sizeof(int);

    curtok = tokens + 1;
    for (i = 0; i < arrlen; i++)
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

        datum_size = to_binary(arrtype, jsmntok_to_str(curtok, json_arr), &binary);
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
        if (curtok->type == JSMN_ARRAY || curtok->type == JSMN_OBJECT)
        {
            int end;

            end = curtok->end;
            while (curtok->start <= end) ++curtok;
            /* TODO: DRY with json_to_document */
        }
        else
        {
            ++curtok;
        }
    }

    pfree(tokens);

    *outbuff_ref = outbuff;

    return buffpos;
}

static int
document_to_binary(char *json, char **outbuff_ref)
{
    document doc;
    int natts;
    char *outbuff;
    int  *attr_ids;
    int **attr_id_refs; /* Just sort the pointers, so we can recover original
                           positions */
    int i;
    int data_size;
    int buffpos;

    json_to_document(json, &doc);
    natts = doc.natts;
    attr_ids = palloc0(natts * sizeof(int));
    attr_id_refs = palloc0(natts * sizeof(int*));

    outbuff = *outbuff_ref;

    for (i = 0; i < natts; i++)
    {
        const char *type;

        // elog(WARNING, "Key: %s",  doc.keys[i]);
        // elog(WARNING, "type: %d", doc.types[i]);
        // elog(WARNING, "value: %s",  doc.values[i]);

        type = get_pg_type(doc.types[i], doc.values[i]);
        attr_ids[i] = get_attribute_id(doc.keys[i], type);
        if (attr_ids[i] < 0)
        {
            attr_ids[i] = add_attribute(doc.keys[i], type);
        }
        attr_id_refs[i] = attr_ids + i;
    }
    qsort(attr_id_refs, natts, sizeof(int*), intref_comparator);

    data_size = 2 * natts * sizeof(int) + 1024; /* arbitrary initial value */
    buffpos = 0;
    outbuff = palloc0(data_size);
    memcpy(outbuff, &natts, sizeof(int));
    buffpos += sizeof(int);
    for (i = 0; i < natts; i++)
    {
        memcpy(outbuff + buffpos, attr_id_refs[i], sizeof(int));
        buffpos += sizeof(int);
    }
    /* Copy data and offsets */
    buffpos = sizeof(int) + 2 * sizeof(int) * natts +
        sizeof(int); /* # attrs, attr_ids, offsets, length */
    for (i = 0; i < natts; i++)
    {
        char *binary;
        int *attr_id_ref;
        int orig_pos;
        int datum_size;

        attr_id_ref = attr_id_refs[i];
        orig_pos = attr_id_ref - attr_ids;
        datum_size = to_binary(doc.types[orig_pos], doc.values[orig_pos],
            &binary);
        if (buffpos + datum_size >= data_size)
        {
            data_size = 2 * (buffpos + datum_size) + 1;
            outbuff = repalloc(outbuff, data_size);
        }
        memcpy(outbuff + buffpos, binary, datum_size);
        memcpy(outbuff + (1 + natts + i) * sizeof(int), &buffpos,
            sizeof(int)); /* # attrs, attr_ids, ith offset */
        buffpos += datum_size;

        /* Cleanup */
        pfree(binary);
    }
    memcpy(outbuff + (1 + 2 * natts) * sizeof(int), &buffpos, sizeof(int));

    pfree(attr_ids);
    pfree(attr_id_refs);

    *outbuff_ref = outbuff;

    return buffpos;
}

static int
to_binary(json_typeid typeid, char *value, char **outbuff_ref)
{
    char *outbuff;

    outbuff = *outbuff_ref;

    switch (typeid)
    {
    case STRING:
        outbuff = pstrndup(value, strlen(value));
        *outbuff_ref = outbuff;
        return strlen(value);
    case INTEGER:
        /* NOTE: I don't think that throwing everything into a char* matters,
         * as long as the length is correct and I've stored the number in its
         * binary form */
        outbuff = palloc0(sizeof(int));
        *((int*)outbuff) = atoi(value);
        *outbuff_ref = outbuff;
        return sizeof(int);
    case FLOAT:
        outbuff = palloc0(sizeof(double));
        *((double*)outbuff) = atof(value);
        *outbuff_ref = outbuff;
        return sizeof(double);
    case BOOLEAN:
        outbuff = palloc0(1);
        if (!strcmp(value, "true")) {
            *outbuff = 1;
        } else if (!strcmp(value, "false")) {
            *outbuff = 0;
        } else {
            elog(WARNING, "document: boolean has invalid value");
            return -1;
        }
        *outbuff_ref = outbuff;
        return 1;
    case DOCUMENT:
        return document_to_binary(value, outbuff_ref);
    case ARRAY:
        return array_to_binary(value, outbuff_ref);
    case NONE:
    default:
        elog(WARNING, "document: invalid data type");
        return -1;
    }
    return -1; /* To shut up compiler warnings */
}

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

static void
binary_to_document(char *binary, document *doc)
{
    int natts;
    int buffpos;
    char **keys;
    json_typeid *types;
    char **values;
    int i; /* Loop variable */

    assert(binary);

    memcpy(&natts, binary, sizeof(int));
    buffpos = sizeof(int);

    keys = palloc0(natts * sizeof(char*));
    values = palloc0(natts * sizeof(char*));
    types = palloc0(natts * sizeof(json_typeid));
    for (i = 0; i < natts; i++)
    {
        int id;
        char *key_string;
        char *type_string;

        memcpy(&id, binary + buffpos, sizeof(int));
        get_attribute(id, &key_string, &type_string);
        elog(WARNING, "Got attribute info: %s, %s", key_string, type_string);

        keys[i] = pstrndup(key_string, strlen(key_string));
        types[i] = get_json_type(type_string);

        pfree(key_string);
        pfree(type_string);

        buffpos += sizeof(int);
    }
    elog(WARNING, "Got ids, keys, and types");

    for (i = 0; i < natts; i++)
    {
        int start, end;
        char *value_data;

        memcpy(&start, binary + buffpos, sizeof(int));
        memcpy(&end, binary + buffpos + sizeof(int), sizeof(int));

        value_data = palloc0(end - start);
        memcpy(value_data, binary + start, end - start);
        values[i] = binary_to_string(types[i], value_data, end - start);
        // elog(WARNING, "key: %s", keys[i]);
        // elog(WARNING, "type: %d", types[i]);
        // elog(WARNING, "value: %s", values[i]);

        pfree(value_data);

        buffpos += sizeof(int);
    }

    doc->natts = natts;
    doc->keys = keys;
    doc->types = types;
    doc->values = values;

    /* FIXME: technically should free all the memory */
}

static char *
binary_document_to_string(char *binary)
{
    document doc;
    int natts;
    char *result;
    int result_size, result_maxsize;
    int i; /* Loop variable */

    binary_to_document(binary, &doc);
    natts = doc.natts;

    result_size = 3; /* {\n} */
    result_maxsize = 64; // TODO: #define
    result = palloc0(result_maxsize + 1);
    strcat(result, "{\n");

    for (i = 0; i < natts; i++)
    {
        char *key;
        char *value;
        char *attr;
        int attr_len;

        key = doc.keys[i];
        value = doc.values[i];

        attr_len = strlen(key) + strlen(value) + 5; /* "k":v,\n" */
        attr = palloc0(attr_len + 1);
        sprintf(attr, "\"%s\":%s,\n", key, value);

        if (result_size + attr_len + 1 >= result_maxsize)
        {
            result_maxsize = 2 * (result_size + attr_len) + 1;
            result = repalloc(result, result_maxsize + 1);
        }
        strcat(result, attr);
    }
    strcat(result, "}"); /* There is space because we keep adding an extra bit
                            to result_maxsize */

    return result;
}

static char *
binary_array_to_string(char *binary)
{
    int buffpos;
    int natts;
    json_typeid type;
    char *result;
    int result_size, result_maxsize;
    int i; /* Loop variable */

    assert(binary);

    memcpy(&natts, binary, sizeof(int));
    memcpy(&type, binary + sizeof(int), sizeof(int));
    buffpos = 2 * sizeof(int);

    result_size = 2; /* '[]' */
    result_maxsize = 64;
    result = palloc0(result_maxsize + 1);
    strcat(result, "[");

    for (i = 0; i < natts; i++) {
        int elt_size;
        char *elt;

        memcpy(&elt_size, binary + buffpos, sizeof(int));
        if (result_size + elt_size + 2 + 1 >= result_maxsize) {
            result_maxsize = 2 * (result_size + elt_size + 2) + 1;
            result = repalloc(result, result_maxsize + 1);
        }

        result_size += sizeof(int);
        buffpos += sizeof(int);

        if (i != 0) {
           strcat(result, ", ");
        }

        elt = palloc0(elt_size);
        memcpy(elt, binary + buffpos, elt_size);
        strcat(result, binary_to_string(type, elt, elt_size));
        pfree(elt);

        result_size += elt_size;
        buffpos += elt_size;
    }
    strcat(result, "]"); /* There is space because we keep adding an extra bit
                            to result_maxsize */

    return result;
}

static char *
binary_to_string(json_typeid type, char *binary, int datum_len)
{
    int i;
    double d;
    char *temp; /* Store temporary result of conversion of raw string */
    char *result;

    assert(binary);

    result = palloc0(datum_len * 8 + 2 + 1); /* Guaranteed to be enough base 10 */

    switch (type)
    {
        case STRING:
            temp = pstrndup(binary, datum_len);
            sprintf(result, "\"%s\"", temp);
            pfree(temp);
            return result;
        case INTEGER:
            assert(datum_len == sizeof(int));
            memcpy(&i, binary, sizeof(int));
            sprintf(result, "%d", i);
            return result;
        case FLOAT:
            assert(datum_len == sizeof(double));
            memcpy(&d, binary, sizeof(double));
            sprintf(result, "%f", d);
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

Datum
document_datum_to_string(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_POINTER(0);
    char *result;

    // TODO: I have the size sitting around here, could use it to verify
    result = binary_document_to_string(datum->vl_dat);

    PG_RETURN_CSTRING(result);
}

/*******************************************************************************
 * Extraction Functions
 ******************************************************************************/

/* NOTE: Because format is known, methods can operate directly on binary data */
Datum document_get(PG_FUNCTION_ARGS);
Datum document_put(PG_FUNCTION_ARGS);
Datum document_delete(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(document_get);
PG_FUNCTION_INFO_V1(document_put);
PG_FUNCTION_INFO_V1(document_delete);

Datum
document_get(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_name = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    const char *data;
    int attr_id;
    int natts;
    char *attr_listing;
    int buffpos;

    data = datum->vl_dat;
    attr_id = get_attribute_id(attr_name, attr_name);

    memcpy(&natts, data, sizeof(int));
    buffpos = sizeof(int);

    attr_listing = NULL;
    attr_listing = bsearch(&attr_id,
                           data + buffpos,
                           natts,
                           sizeof(int),
                           int_comparator);

    // FIXME: that data better be long enough; this code ain't robust
    if (attr_listing)
    {
        int pos;
        int offstart, offend;
        int len;
        json_typeid type;
        char *attr_data;
        int i;
        double d;
        bytea *datum;

        pos = (attr_listing - data) / sizeof(int);
        buffpos += natts * sizeof(int);
        memcpy(&offstart, data + buffpos + pos * sizeof(int), sizeof(int));
        memcpy(&offend, data + (pos + 1) * sizeof(int), sizeof(int));
        len = offend - offstart;

        attr_data = palloc0(len + 1);
        memcpy(attr_data, data + offstart, len);
        attr_data[len] = '\0';
        type = get_json_type(attr_pg_type);

        switch (type)
        {
        case STRING:
             PG_RETURN_CSTRING(pstrndup(attr_data, len));
        case INTEGER:
             assert(len == sizeof(int));
             memcpy(&i, attr_data, sizeof(int));
             PG_RETURN_INT64(i);
        case FLOAT:
             assert(len == sizeof(double));
             memcpy(&d, attr_data, sizeof(double));
             PG_RETURN_FLOAT8(i);
        case BOOLEAN:
             assert(len == sizeof(int));
             memcpy(&i, attr_data, sizeof(int));
             PG_RETURN_BOOL(i);
        case DOCUMENT:
             // TODO: DRY this
             datum = palloc0(VARHDRSZ + len);
             SET_VARSIZE(datum, VARHDRSZ + len);
             memcpy(datum->vl_dat, attr_data, len);
             PG_RETURN_POINTER(datum);
        case ARRAY:
             // FIXME: if text; need to convert to array of text
             // FIXME: store strings as struct text *
             // http://doxygen.postgresql.org/arrayfuncs_8c_source.html#l02865
             // http://www.postgresql.org/message-id/3e3c86f90802281153o15e70724u425cdd0173bb2373@mail.gmail.com
             PG_RETURN_ARRAYTYPE_P(attr_data);
        case NONE:
        default:
             elog(DEBUG5, "Had a null attribute type in the document schema");
             PG_RETURN_NULL();
        }
    }
    else
    {
        PG_RETURN_NULL();
    }
}

Datum
document_delete(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_name = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    char *data;
    int attr_id;
    int natts;
    char *attr_listing;
    char *outdata;
    int attr_pos; /* Position of attribute in header */

    data = datum->vl_dat;
    attr_id = get_attribute_id(attr_name, attr_pg_type);

    memcpy(&natts, data, sizeof(int));

    attr_listing = NULL;
    attr_listing = bsearch(&attr_id,
                           data + sizeof(int),
                           natts,
                           sizeof(int),
                           int_comparator);
    attr_pos = (attr_listing - data - sizeof(int)) / sizeof(int);

    if (attr_listing)
    {
        int datalen; /* Length of original data */
        int outpos; /* Offset of how much data is filled in outbuffer */
        int new_natts; /* Decremented number of attributes */
        int off0; /* Start of data part of document */
        int offstart, offend;
        int len;
        int i;

        outpos = 0;

        memcpy(&offstart, attr_listing + (natts)  * sizeof(int), sizeof(int));
        memcpy(&offend, attr_listing + (natts + 1) * sizeof(int), sizeof(int));
        len = offend - offstart;

        memcpy(&datalen, data + 2 * natts + 1, sizeof(int));
        outdata = palloc0(datalen - 2 * sizeof(int) - len);

        /* Decrement number of attributes */
        new_natts = natts - 1;
        memcpy(outdata, &new_natts, sizeof(int));
        outpos = sizeof(int);
        /* Copy attr ids before current */
        memcpy(outdata + outpos, data + sizeof(int), attr_pos * sizeof(int));
        outpos += attr_pos * sizeof(int);
        /* Copy remaining attr ids */
        memcpy(outdata + outpos,
               attr_listing + sizeof(int),
               (natts - attr_pos - 1) * sizeof(int));
        outpos += (natts - attr_pos - 1) * sizeof(int);
        /* Copy first pos offsets */
        memcpy(outdata + outpos,
               data + (natts + 1) * sizeof(int),
               attr_pos * sizeof(int));
        for (i = attr_pos + 1; i <= natts; i++) { /* <= b/c length appended at
                                                     end */
            int new_offs;

            memcpy(&new_offs,
                   data + (natts + 1) + i * sizeof(int),
                   sizeof(int));
            new_offs -= 2 * sizeof(int) - len;

            memcpy(outdata + outpos, &new_offs, sizeof(int));

            outpos += sizeof(int);
        }
        off0 = 2 * natts + 1;
        memcpy(outdata + outpos, data + off0, offstart - off0);
        outpos += offstart - off0;
        memcpy(outdata + outpos, data + offend, datalen - offend);

        PG_RETURN_POINTER(outdata);
    }
    else
    {
        PG_RETURN_POINTER(data);
    }
}

/* Use this to downgrade */
// NOTE: Assumes attribute does not exist
Datum
document_put(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_name = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    char *attr_value_str = (char*)PG_GETARG_CSTRING(3);
    char *data;
    json_typeid attr_json_typeid;
    char *attr_binary;
    int attr_id;
    int attr_size;
    char *outdata;
    int natts, newnatts;
    int datasize, out_datasize;
    int datapos, outdatapos;
    int offstart, offend, off0; /* Offset to binary for attr, end of it, start
                                   of binary data in document */
    int attr_pos;
    int i; /* Loop variable */
    bytea* out_datum;

    data = datum->vl_dat;
    attr_id = get_attribute_id(attr_name, attr_pg_type);

    if (attr_id < 0)
    {
        attr_id = add_attribute(attr_name, attr_pg_type);
    }

    memcpy(&natts, data, sizeof(int));
    newnatts = natts - 1;
    memcpy(&datasize, data + (2 * natts + 1) * sizeof(int), sizeof(int));

    attr_json_typeid = get_json_type(attr_pg_type);
    attr_size = to_binary(attr_json_typeid, attr_value_str, &attr_binary);
    outdata = palloc0(datasize + 2 * sizeof(int) + attr_size);

    memcpy(outdata, &newnatts, sizeof(int));
    datapos = outdatapos = sizeof(int);
    for (attr_pos = 0; attr_pos < natts; attr_pos++)
    {
        int cur_attr_id;
        memcpy(&cur_attr_id,
               data + datapos + attr_pos * sizeof(int),
               sizeof(int));
        // TODO: custom bsearch, but right now, w/e
        if (cur_attr_id > attr_id)
        {
            break;
        }
    }

    /* Insert aid */
    memcpy(outdata + outdatapos, data + datapos, attr_pos * sizeof(int));
    outdatapos += attr_pos + sizeof(int);
    datapos += attr_pos + sizeof(int);
    memcpy(outdata + outdatapos, &attr_id, sizeof(int));
    outdatapos += sizeof(int);
    memcpy(outdata + outdatapos,
           data + datapos,
           (natts - attr_pos) * sizeof(int));
    outdatapos += (natts - attr_pos) * sizeof(int);
    datapos += (natts - attr_pos) * sizeof(int);

    /* Increment all offsets and insert */
    // TODO: this has got to be messed up
    memcpy(outdata + outdatapos, data + datapos, (attr_pos + 1) * sizeof(int));
    outdatapos += (attr_pos + 1) * sizeof(int);
    memcpy(&offstart, data + datapos + attr_pos * sizeof(int), sizeof(int));
    offend = offstart + attr_size;
    memcpy(outdata + outdatapos, &offend, sizeof(int));
    outdatapos += sizeof(int);
    for (i = attr_pos + 1; i <= natts; i++)
    {
        memcpy(&offstart, data + datapos + i * sizeof(int), sizeof(int));
        offend = offstart + attr_size;
        memcpy(outdata + outdatapos, &offend, sizeof(int));
        outdatapos += sizeof(int);
    }

    /* Insert data */
    off0 = (2 * natts + 1) * sizeof(int);
    memcpy(outdata + outdatapos, data + off0, offstart - off0);
    outdatapos += offstart - off0;
    memcpy(outdata + outdatapos, attr_binary, attr_size);
    outdatapos += attr_size;
    memcpy(outdata + outdatapos, data + offstart,  datasize - offstart);

    out_datasize = datasize + 2 * sizeof(int) + attr_size;
    out_datum = palloc0(VARHDRSZ + out_datasize);
    SET_VARSIZE(out_datum, out_datasize);
    memcpy(out_datum->vl_dat, outdata, out_datasize);

    PG_RETURN_POINTER(out_datum);
}
