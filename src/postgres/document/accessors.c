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
static int document_put_internal(char *doc,
                                 int size,
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

    // elog(WARNING, "in make datum");

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

    // elog(WARNING, "In array get internal");
    // elog(WARNING, "index: %d", index);
    // elog(WARNING, "type: %d", type);
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
        // elog(WARNING, "extracting array element");
        return make_datum(attr_data, itemlen, type, is_null);
    }
    else /* That means we need to keep traversing path */
    {
        char **path;
        char *path_arr_index_map;
        int path_depth;
        char *subpath;

        // elog(WARNING, "attr_path: %s", attr_path);
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
            // elog(WARNING, "subpath in arr: %s", subpath);
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

    // elog(WARNING, "%s", attr_path);
    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    // elog(WARNING, "parsed attr_path");
    /* NOTE: Technically, I just want the first part; I don't need to parse
       the whole thing */

    if (path_depth == 0)
    {
        *is_null = true;
        return (Datum)0;
    }

    // elog(WARNING, "attr name: %s", path[0]);
    // elog(WARNING, "path depth: %d", path_depth);
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
    // elog(WARNING, "found listing");
    // elog(WARNING, "attr id %d", attr_id);
    // elog(WARNING, "natts %d", natts);

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

        // elog (WARNING, "path depth: %d", path_depth);

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
                // elog(WARNING, "subpath: %s", subpath);
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
            // elog(WARNING, "Got to make datum");
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
    // elog(WARNING, "got retval");
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
    size = VARSIZE(datum) - VARHDRSZ;

    // elog(WARNING, "before delete");
    outsize = document_put_internal(data,
                                    size,
                                    attr_path,
                                    attr_pg_type,
                                    NULL,
                                    0,
                                    &outbinary);
    // elog(WARNING, "after delete");
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

    // elog(WARNING, "%s", attr_path);
    path_depth = parse_attr_path(attr_path, &path, &path_arr_index_map);
    // elog(WARNING, "parsed attr_path");
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
    // elog(WARNING, "attr pos: %d", attr_pos);
    // elog(WARNING, "attr exists?: %d", attr_exists);

    /* Check for deletion */
    if (!attr_exists && !attr_binary)
    {
        return -1;
    }

    outitem = NULL;
    if (path_depth > 1)
    {
        if (attr_exists && (type == ARRAY || type == DOCUMENT))
        {
            int start, end;
            char *subpath;

            start = *(int*)(doc + (1 + natts + attr_pos) * sizeof(int));
            end = *(int*)(doc + (1 + natts + attr_pos + 1) * sizeof(int));
            old_item_size = end - start;

            if (type == ARRAY)
            {
                elog(WARNING, "Do not support inserting into/deleting from an array");
                return -1;
            }
            else if (type == DOCUMENT)
            {
                if (path_arr_index_map[1] == true)
                {
                    elog(WARNING, "document_put: not document key invalid path - %s", attr_path);
                    return -1;
                }
                subpath = strchr(attr_path, '.') + 1;
                item_size = document_put_internal(doc + start,
                                                  old_item_size,
                                                  subpath,
                                                  attr_pg_type,
                                                  attr_binary,
                                                  attr_size,
                                                  &outitem);
            }

            if (item_size < 0)
            {
                return -1;
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

    new_size = size;
    if (outitem)
    {
        new_size += item_size + 2 * sizeof(int);
    }
    // elog(WARNING, "size: %d", size);
    // elog(WARNING, "item_size: %d", item_size);
    // elog(WARNING, "old_item_size: %d", old_item_size);
    new_natts = natts + 1;
    if (attr_exists)
    {
        new_size -= old_item_size;
        new_size -= 2 * sizeof(int);
        --new_natts;
    }
    // elog(WARNING, "new size: %d", new_size);
    *outbinary = palloc0(new_size);
    *(int*)(*outbinary) = new_natts - !(outitem);
    buffpos = sizeof(int);
    outbuffpos = sizeof(int);
    for (i = 0; i < new_natts; ++i)
    {
        if (i == attr_pos && !attr_exists)
        {
            memcpy(*outbinary + outbuffpos, &attr_id, sizeof(int));
        }
        else if (i == attr_pos && !outitem) /* Deletion */
        {
            if (attr_exists) /* Should never fail b/c check above */
            {
                buffpos += sizeof(int);
            }
            continue;
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

        start = *(int*)(doc + buffpos);
        if (outitem)
        {
            if (!attr_exists)
            {
                start += 2 * sizeof(int);
            }
        }
        else
        {
            start -= 2 * sizeof(int);
        }

        if (i > attr_pos)
        {
            start += item_size - old_item_size;
        }

        if (outitem || i != attr_pos)
        {
            // elog(WARNING, "start: %d", start);
            memcpy(*outbinary + outbuffpos, &start, sizeof(int));
            outbuffpos += sizeof(int);
        }

        if (i == attr_pos)
        {
            if (outitem)
            {
                memcpy(*outbinary + start, outitem, item_size);
            }
            if (attr_exists) /* FIXME: will this ever fail? */
            {
                buffpos += sizeof(int);
            }
        }
        else
        {
            int doc_start, doc_end;

            doc_start = *(int*)(doc + buffpos);
            doc_end = *(int*)(doc + buffpos + sizeof(int));
            memcpy(*outbinary + start, doc + doc_start, doc_end - doc_start);
            buffpos += sizeof(int);
        }
    }
    memcpy(*outbinary + outbuffpos, &new_size, sizeof(int));
    // elog(WARNING, "size: %d", new_size);
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
    // elog(WARNING, "json type: %d", type);
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
