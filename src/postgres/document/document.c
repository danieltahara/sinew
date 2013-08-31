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
            if (type == NONE) /* Implicit convention: explicit 'nulls' do not
                                 exist; i.e. don't include the key */
            {
                break;
            }

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
                // DRY
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
        elog(WARNING, "%s", outbuff);
        return strlen(value);
    case INTEGER:
        /* NOTE: I don't think that throwing everything into a char* matters,
         * as long as the length is correct and I've stored the number in its
         * binary form */
        outbuff = palloc0(sizeof(int));
        *((int*)outbuff) = atoi(value);
        *outbuff_ref = outbuff;
        elog(WARNING, "%d", *outbuff);
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
    elog(WARNING, "Natts: %d", natts);
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
        elog(WARNING, "Got attribute info for: %d: %s, %s", id, key_string, type_string);

        keys[i] = pstrndup(key_string, strlen(key_string));
        types[i] = get_json_type(type_string);

        pfree(key_string);
        pfree(type_string);

        buffpos += sizeof(int);
    }
    // elog(WARNING, "Got ids, keys, and types");

    // TODO: combine with above
    for (i = 0; i < natts; i++)
    {
        int start, end;
        char *value_data;

        memcpy(&start, binary + buffpos, sizeof(int));
        memcpy(&end, binary + buffpos + sizeof(int), sizeof(int));

        value_data = palloc0(end - start);
        memcpy(value_data, binary + start, end - start);
        elog(WARNING, "converting value to binary");
        values[i] = binary_to_string(types[i], value_data, end - start);
        elog(WARNING, "key: %s", keys[i]);
        elog(WARNING, "type: %d", types[i]);
        elog(WARNING, "value: %s", values[i]);

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
/* TODO: should honestly make the _get methods take const char* */
static Datum document_get_internal(const char *doc,
                                   char *attr_path,
                                   char *attr_pg_type,
                                   bool *is_null);
static Datum array_get_internal(const char *arr,
                                int index,
                                char *attr_path,
                                char *attr_pg_type,
                                bool *is_null);
static Datum make_datum(char *attr_data,
                        int len,
                        json_typeid type,
                        bool *is_null);
static int document_delete_internal(char *doc,
                                    int size,
                                    char *attr_path,
                                    char *attr_pg_type,
                                    char **outbuff_ref);
static int array_delete_internal(char *arr,
                                 int size,
                                 int index,
                                 char *attr_path,
                                 char *attr_pg_type,
                                 char **outbuff_ref);
static int document_put_internal(char *doc,
                                 int size,
                                 char *attr_path,
                                 char *attr_pg_type,
                                 char *attr_binary,
                                 int attr_size,
                                 char **outbinary);
static int array_put_internal(char *arr,
                              int size,
                              int index,
                              char *attr_path,
                              char *attr_pg_type,
                              char *attr_binary,
                              int attr_size,
                              char **outbinary);
/* TODO: when type is '*' */
Datum document_get(PG_FUNCTION_ARGS);
Datum document_get_int(PG_FUNCTION_ARGS);
Datum document_get_float(PG_FUNCTION_ARGS);
Datum document_get_bool(PG_FUNCTION_ARGS);
Datum document_get_text(PG_FUNCTION_ARGS);
Datum document_get_doc(PG_FUNCTION_ARGS);
Datum document_put(PG_FUNCTION_ARGS);
Datum document_put_int(PG_FUNCTION_ARGS);
Datum document_put_float(PG_FUNCTION_ARGS);
Datum document_put_bool(PG_FUNCTION_ARGS);
Datum document_put_text(PG_FUNCTION_ARGS);
Datum document_put_doc(PG_FUNCTION_ARGS);
Datum document_delete(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(document_get);
PG_FUNCTION_INFO_V1(document_get_int);
PG_FUNCTION_INFO_V1(document_get_float);
PG_FUNCTION_INFO_V1(document_get_bool);
PG_FUNCTION_INFO_V1(document_get_text);
PG_FUNCTION_INFO_V1(document_get_doc);
PG_FUNCTION_INFO_V1(document_put);
PG_FUNCTION_INFO_V1(document_put_int);
PG_FUNCTION_INFO_V1(document_put_float);
PG_FUNCTION_INFO_V1(document_put_bool);
PG_FUNCTION_INFO_V1(document_put_text);
PG_FUNCTION_INFO_V1(document_put_doc);
PG_FUNCTION_INFO_V1(document_delete);

static Datum
make_datum(char *attr_data, int len, json_typeid type, bool *is_null)
{
    int i;
    double d;
    bytea *dd;
    char *s;
    text *t;

    elog(WARNING, "in make datum");

    switch (type)
    {
    case STRING:
         t = palloc0(VARHDRSZ + len + 1);
         SET_VARSIZE(t, VARHDRSZ + len + 1);
         memcpy(t->vl_dat, attr_data, len);
         t->vl_dat[len] = '\0'; /* Not necessary b/c palloc0 */
         return PointerGetDatum(t);
    case INTEGER:
         assert(len == sizeof(int));
         memcpy(&i, attr_data, sizeof(int));
         Int64GetDatum(i);
         return Int64GetDatum(i);
    case FLOAT:
         assert(len == sizeof(double));
         memcpy(&d, attr_data, sizeof(double));
         return Float8GetDatum(d);
    case BOOLEAN:
         assert(len == 1);
         memcpy(&i, attr_data, sizeof(int));
         return BoolGetDatum(i);
    case DOCUMENT:
         dd = palloc0(VARHDRSZ + len);
         SET_VARSIZE(dd, VARHDRSZ + len);
         memcpy(dd->vl_dat, attr_data, len);
         return PointerGetDatum(dd);
    case ARRAY:
         s = binary_array_to_string(attr_data);
         len = strlen(s);
         t = palloc0(VARHDRSZ + len + 1);
         SET_VARSIZE(t, VARHDRSZ + len + 1);
         memcpy(t->vl_dat, s, len);
         return PointerGetDatum(t);
    case NONE: /* Shouldn't happen */
    default:
         *is_null = true;
         return (Datum)0; // Copy of PG_RETURN_NULL();
    }
}

static Datum
array_get_internal(const char *arr,
                   int index,
                   char *attr_path,
                   char *attr_pg_type,
                   bool *is_null)
{
    int arrlen;
    json_typeid type;
    char *attr_data;
    int buffpos;
    int i;
    int itemlen;

    assert(arr);
    assert(attr_path && attr_pg_type);
    memcpy(&arrlen, arr, sizeof(int));
    memcpy(&type, arr + sizeof(int), sizeof(int)); /* NOTE: technically, the method will never be called with a type mismatch */

    elog(WARNING, "In array get internal");
    elog(WARNING, "index: %d", index);
    elog(WARNING, "type: %d", type);
    if (index >= arrlen)
    {
        *is_null = true;
        return (Datum)0;
    }

    buffpos = 2 * sizeof(int);

    for (i = 0; i < index; i++)
    {
        memcpy(&itemlen, arr + buffpos, sizeof(int));
        buffpos += sizeof(int) + itemlen;
    }
    memcpy(&itemlen, arr + buffpos, sizeof(int));
    buffpos += sizeof(int);

    attr_data = palloc0(itemlen + 1);
    memcpy(attr_data, arr + buffpos, itemlen);
    attr_data[itemlen] = '\0'; /* NOTE: not technically necessary */

    if (strlen(attr_path) == 0)
    {
        elog(WARNING, "extracting array element");
        return make_datum(attr_data, itemlen, type, is_null);
    }
    else /* That means we need to keep traversing path */
    {
        char **path;
        char *path_arr_index_map;
        int path_depth;
        char *subpath;

        elog(WARNING, "attr_path: %s", attr_path);
        if (type != DOCUMENT && type != ARRAY)
        {
            *is_null = true;
            return (Datum)0;
        }
        else if (type == DOCUMENT) /* ttr_path has form .\w+(\.\w+|[\d+])* */
        {
            ++attr_path; /* Jump the period */
            path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
            if (path_arr_index_map[0] == true)
            {
                elog(ERROR, "document_get: invalid path - %s", attr_path);
            }
            return document_get_internal(attr_data,
                                         attr_path,
                                         attr_pg_type,
                                         is_null);
        }
        else /* Attr_path has form [\d+]... */
        {
            path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
            if (path_arr_index_map[0] == false)
            {
                elog(ERROR, "document_get: not array index invalid path - %s", attr_path);
            }
            subpath = strchr(attr_path, ']') + 1;
            elog(WARNING, "subpath in arr: %s", subpath);
            return array_get_internal(attr_data,
                                      strtol(path[0], NULL, 10),
                                      subpath,
                                      attr_pg_type,
                                      is_null);
        }
    }
}

static Datum
document_get_internal(const char *doc,
                      char *attr_path,
                      char *attr_pg_type,
                      bool *is_null)
{
    int attr_id;
    json_typeid type;
    int natts;
    char *attr_listing;
    int buffpos;
    char **path;
    char *path_arr_index_map;
    int path_depth;

    elog(WARNING, "%s", attr_path);
    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    elog(WARNING, "parsed attr_path");
    /* NOTE: Technically, I just want the first part; I don't need to parse
       the whole thing */

    if (path_depth == 0)
    {
        *is_null = true;
        return (Datum)0;
    }

    elog(WARNING, "attr name: %s", path[0]);
    elog(WARNING, "path depth: %d", path_depth);
    if (path_depth > 1)
    {
        const char *pg_type;
        pg_type = get_pg_type_for_path(path,
                                       path_arr_index_map,
                                       path_depth,
                                       attr_pg_type);
        attr_id = get_attribute_id(path[0], pg_type);
        type = get_json_type(pg_type);
    }
    else
    {
        attr_id = get_attribute_id(path[0], attr_pg_type);
        type = get_json_type(attr_pg_type);
    }

    memcpy(&natts, doc, sizeof(int));
    buffpos = sizeof(int);

    attr_listing = NULL;
    attr_listing = bsearch(&attr_id,
                           doc + buffpos,
                           natts,
                           sizeof(int),
                           int_comparator);
    elog(WARNING, "found listing");
    elog(WARNING, "attr id %d", attr_id);
    elog(WARNING, "natts %d", natts);

    if (attr_listing) /* TODO: separate attr_listing == NULL */
    {
        int pos;
        int offstart, offend;
        int len;
        char *attr_data;
        char *subpath; /* In the case of a nested doc or array */

        pos = (attr_listing - buffpos - doc) / sizeof(int);
        buffpos += natts * sizeof(int);
        memcpy(&offstart, doc + buffpos + pos * sizeof(int), sizeof(int));
        memcpy(&offend, doc + buffpos + (pos + 1) * sizeof(int), sizeof(int));
        len = offend - offstart;

        attr_data = palloc0(len + 1);
        memcpy(attr_data, doc + offstart, len);
        attr_data[len] = '\0';

        elog (WARNING, "path depth: %d", path_depth);

        if (path_depth > 1)
        {
            if (type == DOCUMENT)
            {
                if (path_arr_index_map[1] == true)
                {
                    elog(ERROR, "document_get: invalid path - %s", attr_path);
                }
                subpath = strchr(attr_path, '.') + 1;
                return document_get_internal(attr_data,
                                             subpath,
                                             attr_pg_type,
                                             is_null);
            }
            else if (type == ARRAY)
            {
                if (path_arr_index_map[1] == false)
                {
                    elog(ERROR, "document_get: invalid path - %s", attr_path);
                }
                subpath = strchr(attr_path, ']') + 1;
                elog(WARNING, "subpath: %s", subpath);
                /* NOTE: Might be memory issues with very deep nesting */
                return array_get_internal(attr_data,
                                          strtol(path[1], NULL, 10),
                                          subpath,
                                          attr_pg_type,
                                          is_null);
            }
            else
            {
                *is_null = true;
                return (Datum)0;
            }
        }
        else
        {
            elog(WARNING, "Got to make datum");
            return make_datum(attr_data, len, type, is_null);
        }
    }
    else
    {
        *is_null = true;
        return (Datum)0;
    }
}

Datum
document_get(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   attr_pg_type,
                                   &is_null);
    elog(WARNING, "got retval");
    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        char *strval;
        bytea *text_datum;
        /* This is super inefficient, but w/e */
        switch(get_json_type(attr_pg_type))
        {
        case STRING:
        case ARRAY:
            return retval;
        case INTEGER:
            strval = palloc0(101); /* TODO: what is range */
            sprintf(strval, "%d", (int)retval);
            break;
        case FLOAT:
            strval = palloc0(101);
            sprintf(strval, "%f", (double)retval);
            break;
        case BOOLEAN:
            strval = palloc0(6);
            sprintf(strval, "%s", ((int)retval ? "true" : "false"));
            break;
        case DOCUMENT:
            strval = binary_document_to_string(((bytea*)retval)->vl_dat);
            break;
        case NONE:
        default:
            PG_RETURN_NULL();
        }
        text_datum = palloc0(VARHDRSZ + strlen(strval));
        SET_VARSIZE(text_datum, VARHDRSZ + strlen(strval));
        memcpy(text_datum->vl_dat, strval, strlen(strval));
        pfree(strval);
        return PointerGetDatum(text_datum);
    }
}

Datum
document_get_int(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   INTEGER_TYPE,
                                   &is_null);

    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        return retval;
    }
}

Datum
document_get_float(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   FLOAT_TYPE,
                                   &is_null);

    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        return retval;
    }
}

Datum
document_get_bool(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   BOOLEAN_TYPE,
                                   &is_null);

    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        return retval;
    }
}

Datum
document_get_text(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   STRING_TYPE,
                                   &is_null);

    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        return retval;
    }
}

Datum
document_get_doc(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    Datum retval;
    bool is_null;

    is_null = false;
    retval = document_get_internal(datum->vl_dat,
                                   attr_path,
                                   DOCUMENT_TYPE,
                                   &is_null);

    if (is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        return retval;
    }
}

static int
array_delete_internal(char *arr,
                      int size,
                      int index,
                      char *attr_path,
                      char *attr_pg_type,
                      char **outbuff_ref)
{
    int arrlen;
    json_typeid type;
    int arrpos;
    int i;
    int item_size;
    int path_depth;
    char **path;
    char *path_arr_index_map;

    assert(arr);
    assert(attr_path && attr_pg_type);
    memcpy(&arrlen, arr, sizeof(int));
    memcpy(&type, arr + sizeof(int), sizeof(int));

    if (index >= arrlen)
    {
        return -1;
    }

    arrpos = 2 * sizeof(int);
    for (i = 0; i < index; i++)
    {
        item_size = *(int*)(arr + arrpos);
        arrpos += sizeof(int) + item_size;
    }
    item_size = *(int*)(arr + arrpos);

    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    if (path_depth > 0)
    {
        if (type == DOCUMENT) /* attr_path has form .\w+(\.\w+|[\d+])* */
        {
            ++attr_path; /* Jump the period */
            if (path_arr_index_map[0] == true)
            {
                elog(ERROR, "document_get: invalid path - %s", attr_path);
            }
            return document_delete_internal(arr + arrpos + sizeof(int),
                                            item_size,
                                            attr_path,
                                            attr_pg_type,
                                            outbuff_ref);
        }
        else if (type == ARRAY) /* Attr_path has form [\d+]... */
        {
            char *subpath;

            if (path_arr_index_map[0] == false)
            {
                elog(ERROR, "document_get: not array index invalid path - %s", attr_path);
            }
            subpath = strchr(attr_path, ']') + 1;
            return array_delete_internal(arr + arrpos + sizeof(int),
                                         item_size,
                                         strtol(path[0], NULL, 10),
                                         subpath,
                                         attr_pg_type,
                                         outbuff_ref);
        }
        else
        {
            return -1;
        }
    }
    else
    {
        int new_size;

        new_size = size - item_size - sizeof(int);
        *outbuff_ref = palloc0(new_size);
        memcpy(*outbuff_ref, arr, arrpos);
        memcpy(*outbuff_ref + arrpos + sizeof(int) + item_size, arr, new_size - arrpos);
        *(int*)(*outbuff_ref) = arrlen - 1;
        return new_size;
    }
}

static int
document_delete_internal(char *doc,
                         int size,
                         char *attr_path,
                         char *attr_pg_type,
                         char **outbuff_ref)
{
    int natts;
    int attr_id;
    char *attr_listing;
    int attr_pos; /* Position of attribute in header */
    int attr_start, attr_end;
    int attr_size;
    int path_depth;
    char **path;
    char *path_arr_index_map;
    json_typeid type;

    elog(WARNING, "%s", attr_path);
    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    elog(WARNING, "parsed attr_path");
    if (path_depth == 0)
    {
        return -1;
    }

    if (path_depth > 1)
    {
        const char *pg_type;

        pg_type = get_pg_type_for_path(path,
                                       path_arr_index_map,
                                       path_depth,
                                       attr_pg_type);
        attr_id = get_attribute_id(path[0], pg_type);
        type = get_json_type(pg_type);
    }
    else
    {
        attr_id = get_attribute_id(path[0], attr_pg_type);
        type = get_json_type(attr_pg_type); /* Unused, but for symmetry */
    }

    natts = *(int*)(doc);

    attr_listing = NULL;
    attr_listing = bsearch(&attr_id,
                           doc + sizeof(int),
                           natts,
                           sizeof(int),
                           int_comparator);
    if (attr_listing == NULL)
    {
        return -1;
    }

    attr_pos = (attr_listing - doc - sizeof(int)) / sizeof(int);
    attr_start = *(int*)(doc + (natts + 1 + attr_pos) * sizeof(int));
    attr_end = *(int*)(doc + (natts + 1 + attr_pos + 1) * sizeof(int));
    attr_size = attr_end - attr_start;

    if (path_depth > 1)
    {
        char *subpath;

        if (type == ARRAY)
        {
            char *binary;
            int new_arrsize;

            if (path_arr_index_map[1] == false)
            {
                elog(ERROR, "document_get: invalid path - %s", attr_path);
            }
            subpath = strchr(attr_path, ']') + 1;
            new_arrsize = array_delete_internal(doc + attr_start,
                                                attr_size,
                                                strtol(path[1], NULL, 10),
                                                subpath,
                                                attr_pg_type,
                                                &binary);
            if (new_arrsize < 0)
            {
                return -1;
            }
            *outbuff_ref = palloc0(size - attr_size + new_arrsize);
            memcpy(*outbuff_ref, doc, attr_start);
            memcpy(*outbuff_ref + attr_start, binary, new_arrsize);
            memcpy(*outbuff_ref + attr_start + new_arrsize, doc + attr_end, size - attr_end);
            return size - attr_size + new_arrsize;
        }
        else if (type == DOCUMENT)
        {
            char *binary;
            int new_docsize;

            if (path_arr_index_map[1] == true)
            {
                elog(ERROR, "document_get: invalid path - %s", attr_path);
            }
            subpath = strchr(attr_path, '.') + 1;

            new_docsize = document_delete_internal(doc + attr_start, attr_size, subpath, attr_pg_type, &binary);
            if (new_docsize < 0)
            {
                return -1;
            }
            *outbuff_ref = palloc0(size - attr_size + new_docsize);
            memcpy(*outbuff_ref, doc, attr_start);
            memcpy(*outbuff_ref + attr_start, binary, new_docsize);
            memcpy(*outbuff_ref + attr_start + new_docsize, doc + attr_end, size - attr_end);
            return size - attr_size + new_docsize;
        }
        else
        {
            return -1;
        }
    }
    else /* Top-level attribute */
    {
        int i;

        *outbuff_ref = palloc0(size - attr_size - 2 * sizeof(int));
        memcpy(*outbuff_ref, doc, (1 + attr_pos) * sizeof(int));
        memcpy(*outbuff_ref, doc + (1 + attr_pos + 1) * sizeof(int), (natts - 1) * sizeof(int));
        memcpy(*outbuff_ref, doc + (1 + natts + attr_pos + 1) * sizeof(int), attr_start - (1 + natts + attr_pos + 1) * sizeof(int));
        for (i = attr_pos; i < natts - attr_pos; ++i)
        {
            *(int*)(*outbuff_ref + (1 + natts - 1 + i) * sizeof(int)) -= attr_size;
        }
        memcpy(*outbuff_ref + attr_start - 2 * sizeof(int), doc + attr_end, size - attr_end);
        *(int*)(*outbuff_ref) = natts - 1;

        return size - attr_size;
    }
}

Datum
document_delete(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea *outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum);

    elog(WARNING, "before delete");
    outsize = document_delete_internal(data, size, attr_path, attr_pg_type, &outbinary);
    elog(WARNING, "after delete");
    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);

    PG_RETURN_POINTER(outdatum);
}

static int
array_put_internal(char *arr,
                   int size,
                   int index,
                   char *attr_path,
                   char *attr_pg_type,
                   char *attr_binary,
                   int attr_size,
                   char **outbinary)
{
    int arrlen;
    json_typeid type;
    int arrpos;
    int i;
    int item_size;
    int path_depth;
    char **path;
    char *path_arr_index_map;
    char *outitem;

    assert(arr);
    assert(attr_path && attr_pg_type);
    memcpy(&arrlen, arr, sizeof(int));
    memcpy(&type, arr + sizeof(int), sizeof(int));

    if (index >= arrlen + 1)
    {
        elog(WARNING, "document_put: array index OOB - %d", index);
        return -1;
    }

    arrpos = 2 * sizeof(int);
    for (i = 0; i < index; i++)
    {
        item_size = *(int*)(arr + arrpos);
        arrpos += sizeof(int) + item_size;
    }
    item_size = *(int*)(arr + arrpos);

    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    if (path_depth > 0)
    {
        if (type == DOCUMENT) /* attr_path has form .\w+(\.\w+|[\d+])* */
        {
            ++attr_path; /* Jump the period */
            if (path_arr_index_map[0] == true)
            {
                elog(ERROR, "document_put: invalid path - %s", attr_path);
            }
            return document_put_internal(arr + arrpos + sizeof(int),
                                         item_size,
                                         attr_path,
                                         attr_pg_type,
                                         attr_binary,
                                         attr_size,
                                         &outitem);
        }
        else if (type == ARRAY) /* Attr_path has form [\d+]... */
        {
            char *subpath;

            if (path_arr_index_map[0] == false)
            {
                elog(ERROR, "document_put: not array index invalid path - %s", attr_path);
            }
            subpath = strchr(attr_path, ']') + 1;
            return array_put_internal(arr + arrpos + sizeof(int),
                                      item_size,
                                      strtol(path[0], NULL, 10),
                                      subpath,
                                      attr_pg_type,
                                      attr_binary,
                                      attr_size,
                                      &outitem);
        }
        else
        {
            return -1;
        }
    }
    else
    {
        int new_size;

        if (index < arrlen)
        {
            new_size = size + attr_size - item_size;
            *outbinary = palloc0(new_size);
            memcpy(*outbinary, arr, arrpos);
            memcpy(*outbinary + arrpos, &attr_size, sizeof(int));
            memcpy(*outbinary + arrpos + sizeof(int), attr_binary, attr_size);
            memcpy(*outbinary + arrpos + sizeof(int) + attr_size, arr, new_size - arrpos);
            *(int*)(*outbinary) = arrlen;
        }
        else
        {
            new_size = size + attr_size + sizeof(int);
            *outbinary = palloc0(new_size);
            memcpy(*outbinary, arr, size);
            memcpy(*outbinary + size, &attr_size, sizeof(int));
            memcpy(*outbinary + size + sizeof(int), attr_binary, attr_size);
            *(int*)(*outbinary) = arrlen + 1;
        }
        return new_size;
    }
}

static int
document_put_internal(char *doc,
                      int size,
                      char *attr_path,
                      char *attr_pg_type,
                      char *attr_binary,
                      int attr_size,
                      char **outbinary)
{
    int natts, new_natts;
    int attr_id;
    int path_depth;
    char **path;
    char *path_arr_index_map;
    int i;
    bool attr_exists;
    int attr_pos;
    int new_size;
    char *outitem;
    int item_size, old_item_size;
    json_typeid type;
    int buffpos, outbuffpos;

    elog(WARNING, "%s", attr_path);
    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    elog(WARNING, "parsed attr_path");
    if (path_depth == 0)
    {
        return -1;
    }

    if (path_depth > 1)
    {
        const char *pg_type;

        pg_type = get_pg_type_for_path(path,
                                       path_arr_index_map,
                                       path_depth,
                                       attr_pg_type);
        attr_id = get_attribute_id(path[0], pg_type);
        if (attr_id < 0)
        {
            elog(WARNING, "document_get: cannot put because container does not exist - %s", path[0]);
            return -1;
        }

        type = get_json_type(pg_type);
    }
    else
    {
        attr_id = get_attribute_id(path[0], attr_pg_type);
        if (attr_id < 0)
        {
            attr_id = add_attribute(path[0], attr_pg_type);
        }

        type = get_json_type(attr_pg_type); /* Unused, but for symmetry */
    }

    natts = *(int*)(doc);
    /* Locate aid, if exists */

    attr_exists = false;
    for (i = 0; i < natts; ++i)
    {
        int id;

        id = *(int*)(doc + sizeof(int) + i * sizeof(int));
        if (attr_id == id)
        {
            attr_exists = true;
            break;
        }
        else if (attr_id < id)
        {
            break;
        }
    }
    attr_pos = i;
    elog(WARNING, "attr pos: %d", attr_pos);
    elog(WARNING, "attr exists?: %d", attr_exists);

    if (path_depth > 1)
    {
        if (attr_exists && (type == ARRAY || type == DOCUMENT))
        {
            int start, end;

            start = *(int*)(doc + (1 + natts + attr_pos) * sizeof(int));
            end = *(int*)(doc + (1 + natts + attr_pos + 1) * sizeof(int));
            old_item_size = end - start;

            if (type == ARRAY)
            {
                char *subpath;

                if (path_arr_index_map[0] == false)
                {
                    elog(ERROR, "document_put: not array index invalid path - %s", attr_path);
                }
                subpath = strchr(attr_path, ']') + 1;
                item_size = array_put_internal(doc + start,
                                               old_item_size,
                                               strtol(path[0], NULL, 10),
                                               subpath,
                                               attr_pg_type,
                                               attr_binary,
                                               attr_size,
                                               &outitem);
            }
            else if (type == DOCUMENT)
            {
                if (path_arr_index_map[0] == true)
                {
                    elog(ERROR, "document_put: not document key invalid path - %s", attr_path);
                }
                item_size = document_put_internal(doc + start,
                                                  old_item_size,
                                                  attr_path + 1,
                                                  attr_pg_type,
                                                  attr_binary,
                                                  attr_size,
                                                  &outitem);
            }
        }
        else
        {
            assert(false);
            return -1;
        }
    }
    else /* Top-level attribute */
    {
        item_size = attr_size;
        outitem = attr_binary;
        if (attr_exists)
        {
            int start, end;

            start = *(int*)(doc + (1 + natts + attr_pos) * sizeof(int));
            end = *(int*)(doc + (1 + natts + attr_pos + 1) * sizeof(int));
            old_item_size = end - start;
        }
        else
        {
            old_item_size = 0;
        }
    }

    new_size = size + item_size + 2 * sizeof(int);
    elog(WARNING, "size: %d", size);
    elog(WARNING, "item_size: %d", item_size);
    new_natts = natts + 1;
    if (attr_exists)
    {
        new_size -= old_item_size;
        new_size -= 2 * sizeof(int);
        --new_natts;
    }
    *outbinary = palloc0(new_size);
    *(int*)(*outbinary) = new_natts;
    buffpos = sizeof(int);
    outbuffpos = sizeof(int);
    for (i = 0; i < new_natts; ++i)
    {
        if (i == attr_pos && !attr_exists)
        {
            memcpy(*outbinary + outbuffpos, &attr_id, sizeof(int));
        }
        else
        {
            int id;

            id = *(int*)(doc + buffpos);
            memcpy(*outbinary + outbuffpos, &id, sizeof(int));
            buffpos += sizeof(int);
        }
        outbuffpos += sizeof(int);
    }

    for (i = 0; i < new_natts; ++i)
    {
        int start;

        start = *(int*)(doc + buffpos) + (attr_exists ? 0 : 2 * sizeof(int));
        elog(WARNING, "start: %d", start);
        if (i > attr_pos)
        {
            start += item_size - old_item_size;
        }
        memcpy(*outbinary + outbuffpos, &start, sizeof(int));
        outbuffpos += sizeof(int);

        if (i == attr_pos)
        {
            memcpy(*outbinary + start, outitem, item_size);
            if (attr_exists)
            {
                buffpos += sizeof(int);
            }
        }
        else
        {
            int doc_start, doc_end;

            doc_start = *(int*)(doc + buffpos);
            doc_end = *(int*)(doc + buffpos + sizeof(int));
            elog(WARNING, "doc_start: %d, end:%d", doc_start, doc_end);
            memcpy(*outbinary + start, doc + doc_start, doc_end - doc_start);
            buffpos += sizeof(int);
        }
    }
    elog(WARNING, "outbuffpos: %d", outbuffpos);
    memcpy(*outbinary + outbuffpos, &new_size, sizeof(int));
    elog(WARNING, "size: %d", new_size);
    return new_size;
}

/* Use this to downgrade */
Datum
document_put(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    char *attr_pg_type = (char*)PG_GETARG_CSTRING(2);
    char *attr_value = (char*)PG_GETARG_CSTRING(3);
    char *attr_binary;
    int attr_size;
    char *data;
    int size;
    json_typeid type;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum) - VARHDRSZ;

    type = get_json_type(attr_pg_type);
    elog(WARNING, "json type: %d", type);
    attr_size = to_binary(type, attr_value, &attr_binary);
    if (attr_size < 0)
    {
        elog(WARNING, "invalid value: %s", attr_value);
        PG_RETURN_POINTER(datum);
    }
    outsize = document_put_internal(data, size, attr_path, attr_pg_type, attr_binary, attr_size, &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}

Datum
document_put_int(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    int attr_value = PG_GETARG_INT64(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum) - VARHDRSZ;

    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    INTEGER_TYPE,
                                    (char*)&attr_value,
                                    sizeof(int),
                                    &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}

Datum
document_put_float(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    double attr_value = PG_GETARG_FLOAT8(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum) - VARHDRSZ;

    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    FLOAT_TYPE,
                                    (char*)&attr_value,
                                    sizeof(double),
                                    &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}

Datum
document_put_bool(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    bool attr_value = PG_GETARG_BOOL(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum) - VARHDRSZ;

    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    BOOLEAN_TYPE,
                                    (char*)&attr_value,
                                    sizeof(bool),
                                    &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}

Datum
document_put_text(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    char *attr_value = PG_GETARG_CSTRING(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat - VARHDRSZ;
    size = VARSIZE(datum);

    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    STRING_TYPE,
                                    attr_value,
                                    strlen(attr_value),
                                    &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}

Datum
document_put_doc(PG_FUNCTION_ARGS)
{
    bytea *datum = (bytea*)PG_GETARG_BYTEA_P_COPY(0);
    char *attr_path = (char*)PG_GETARG_CSTRING(1);
    bytea *attr_value = PG_GETARG_BYTEA_P_COPY(2);
    char *data;
    int size;
    char *outbinary;
    int outsize;
    bytea* outdatum;

    data = datum->vl_dat;
    size = VARSIZE(datum) - VARHDRSZ;

    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    DOCUMENT_TYPE,
                                    attr_value->vl_dat,
                                    VARSIZE(attr_value) - VARHDRSZ,
                                    &outbinary);

    if (outsize < 0)
    {
        PG_RETURN_POINTER(datum);
    }
    outdatum = palloc0(VARHDRSZ + outsize);
    SET_VARSIZE(outdatum, VARHDRSZ + outsize);
    memcpy(outdatum->vl_dat, outbinary, outsize);
    PG_RETURN_POINTER(outdatum);
}
