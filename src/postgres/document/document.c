#include <postgres.h> /* This include must precede all other postgres
                         dependencies */

#include <assert.h>

#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <utils/snapmgr.h>
#include <utils/array.h>

#include "lib/jsmn/jsmn.h"
#include "utils.h"
#include "schema.h"
#include "document.h"

/*******************************************************************************
 * String -> Binary
 ******************************************************************************/

void
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
                state = KEY;
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
            // elog(WARNING, "%s, %d, %s", keyname, type, value);

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

int
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
    // elog(WARNING, "arr size: %d", arrlen);
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
            // elog(WARNING, "document: inhomogenous types in JSMN_ARRAY: %s", json_arr);
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

int
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

int
to_binary(json_typeid typeid, char *value, char **outbuff_ref)
{
    char *outbuff;

    outbuff = *outbuff_ref;

    switch (typeid)
    {
    case STRING:
        outbuff = pstrndup(value, strlen(value));
        *outbuff_ref = outbuff;
        // elog(WARNING, "%s", outbuff);
        return strlen(value);
    case INTEGER:
        /* NOTE: I don't think that throwing everything into a char* matters,
         * as long as the length is correct and I've stored the number in its
         * binary form */
        outbuff = palloc0(sizeof(int));
        *((int*)outbuff) = atoi(value);
        *outbuff_ref = outbuff;
        // elog(WARNING, "%d", *outbuff);
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

/*******************************************************************************
 * Binary -> Strng
 ******************************************************************************/

void
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
    // elog(WARNING, "Natts: %d", natts);
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
        // elog(WARNING, "Got attribute info for: %d: %s, %s", id, key_string, type_string);

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
        // elog(WARNING, "converting value to binary");
        // elog(WARNING, "start: %d, end: %d", start, end);
        // elog(WARNING, "types[i]: %d", types[i]);
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

char *
binary_document_to_string(char *binary)
{
    document doc;
    int natts;
    char *result;
    int result_size, result_maxsize;
    int i; /* Loop variable */

    binary_to_document(binary, &doc);
    // elog(WARNING, "converted  doc");
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

char *
binary_array_to_string(char *binary)
{
    int buffpos;
    int natts;
    json_typeid type;
    char *result;
    int result_size, result_maxsize;
    int i; /* Loop variable */

    assert(binary);

    // elog(WARNING, "in binary array to string");
    memcpy(&natts, binary, sizeof(int));
    // elog(WARNING, "natts: %d", natts);
    memcpy(&type, binary + sizeof(int), sizeof(int));
    // elog(WARNING, "type: %d", type);
    buffpos = 2 * sizeof(int);

    result_size = 2; /* '[]' */
    result_maxsize = 64;
    result = palloc0(result_maxsize + 1);
    strcat(result, "[");

    // elog(WARNING, "converting binary array to str with natts:%d", natts);
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

char *
binary_to_string(json_typeid type, char *binary, int datum_len)
{
    int i;
    double d;
    char *temp; /* Store temporary result of conversion of raw string */
    char *result;

    assert(binary);

    // elog(WARNING, "in binary to string");
    // elog(WARNING, "type: %d", type);
    // elog(WARNING, "len: %d", datum_len);
    // elog(WARNING, "prod = %d", datum_len * 8 + 2 + 1);
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
            /* pfree(result); Hack to get rid of segfault: in
             * http://d.pr/i/UM8I
             */
            return binary_document_to_string(binary);
        case ARRAY:
            /* pfree(result); Hack to get rid of segfault: in
             * http://d.pr/i/UM8I
             */
            return binary_array_to_string(binary);
        case NONE:
        default:
            elog(ERROR, "document: invalid binary");
    }

}
