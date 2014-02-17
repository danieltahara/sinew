#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "avro.h"
#include "nobench_schema.h"
#include "../json.h"

char dbname[] = "avro_test.db";
const char codec[] = "null"; // Compression?? = null, deflate
char *to_avro_keyname(char *key, json_typeid type, char *value);
int avro_record_value_fill(avro_value_t *avro_value, char *json);
int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_project(FILE *outfile) {
    return 0;
}

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename;
    FILE *infile, *outfile, *extract_outfile;
    int rval; // Function status codes
    clock_t start, diff;
    int msec;
    avro_schema_error_t error;

    // TODO: DRY
    infilename = argv[1];
    infile = fopen(infilename, "r");
    start = clock();
    if (!test_serialize(infile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Serialize: %d ms", msec);
    fclose(infile);

    outfilename = argv[2];
    outfile = fopen(outfilename, "w");
    start = clock();
    if (!test_deserialize(outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Deserialize: %d ms", msec);
    fclose(outfile);

    extract_outfilename = argv[3];
    extract_outfile = fopen(extract_outfilename, "w");
    start = clock();
    if (!test_project(extract_outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Extract: %d ms", msec);
    fclose(extract_outfile);
}

/* Read all the records and print them */
int test_deserialize(FILE *outfile) {
    avro_file_reader_t reader;
    avro_schema_t schema;
    avro_value_iface_t *iface;
    avro_value_t avro_value;
    char *json;
    int rval;

    rval = avro_file_reader(dbname, &reader);
    if (rval) {
        fprintf(stderr, "Error opening file: %s\n", avro_strerror());
        return rval;
    }
    schema = avro_file_reader_get_writer_schema(reader);
    iface = avro_generic_class_from_schema(schema);
    avro_generic_value_new(iface, &avro_value);

    while (avro_file_reader_read_value(reader, &avro_value) == 0) {
        rval = avro_value_to_json(&avro_value, 1, &json);
        if (rval) {
            fprintf(stderr, "Error converting to json: %s\n", avro_strerror());
            return rval;
        }
        fprintf(outfile, "%s\n", json);
        free(json);
    }

    // Cleanup
    avro_file_reader_close(reader);
    avro_value_decref(&avro_value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);

    return 1;
}

/* See: http://dcreager.github.io/avro-examples/resolved-writer.html */
/*
int test_project(FILE *outfile) {
    avro_file_reader_t dbreader;
    avro_schema_t  reader_schema;
    avro_schema_t  writer_schema;
    avro_value_iface_t  *writer_iface;
    avro_value_iface_t  *reader_iface;
    avro_value_t  writer_value;
    avro_value_t  reader_value;
    int rval;
    const char *value;
    size_t size;

    rval = avro_file_reader(dbname, &dbreader);
    if (rval) {
        fprintf(stderr, "Error opening file: %s\n", avro_strerror());
        return rval;
    }
    writer_schema = avro_file_reader_get_writer_schema(dbreader);
    reader_iface = avro_generic_class_from_schema(projected_schema);
    avro_generic_value_new(reader_iface, &reader_value);
    writer_iface = avro_resolved_writer_new(writer_schema, reader_schema);
    avro_resolved_writer_new_value(writer_iface, &writer_value);
    avro_resolved_writer_set_dest(&writer_value, &reader_value);

    while (avro_file_reader_read_value(dbreader, &writer_value) == 0) {
        avro_value_t field;

        avro_value_get_by_name(&reader_value, PROJECTED_KEY, &field, NULL);
        if (rval) {
            fprintf(stderr, "Error reading: %s\n", avro_strerror());
            return rval;
        }

        rval = avro_value_get_string(&field, &value, &size);
        if (rval) {
            fprintf(stderr, "Error converting to string: %s\n", avro_strerror());
            return rval;
        }
        fprintf(outfile, "%s\n", value);
        // free(value);
    }

    avro_file_reader_close(dbreader);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);
    avro_value_decref(&reader_value);
    avro_value_iface_decref(reader_iface);
    avro_schema_decref(reader_schema);

    return 1;
}
*/

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    avro_schema_t schema;
    avro_schema_error_t error;
    avro_file_writer_t db;
    avro_value_iface_t *iface;
    avro_value_t avro_value;

    char *buffer;
    size_t len, read;
    int rval;

    // Parse schema into a schema data structure
    if ((rval = avro_schema_from_json(NOBENCH_SCHEMA, 0, &schema, &error))) {
        fprintf(stderr, "Unable to parse nobench schema\n");
        return rval;
    }

    iface = avro_generic_class_from_schema(schema);
    rval = avro_generic_value_new(iface, &avro_value);

    if ((rval = avro_file_writer_create_with_codec(dbname, schema, &db, codec, 13000 * 1024))) {
        fprintf(stderr, "Error creating %s: %s\n", dbname, avro_strerror());
        return rval;
    }
    avro_file_writer_flush(db);

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        avro_value_t field;
        size_t index;
        const char *dummy;

        /*
        avro_value_get_by_name(&avro_value, "num_int", &field, &index);
        fprintf(stderr, "rval: %d at index %ld\n", rval, index);
        rval = avro_value_set_int(&field, 64);
        rval = avro_value_get_int(&field, &index);
        fprintf(stderr, "rval: %d\n value: %ld\n", rval, index);
        avro_value_get_by_name(&avro_value, "str1_str", &field, &index);
        rval = avro_value_set_string(&field, "hello");
        rval = avro_value_get_string(&field, &dummy, NULL);
        fprintf(stderr, "rval: %d\n value: %s\n", rval, dummy);
        index = -1;
        */
        // Create a new record
        avro_record_value_fill(&avro_value, buffer);
        // avro_value_get_size(&avro_value, &index);
        // fprintf(stderr, "record has %zu attr\n", index);

        // Write the record
        rval = avro_file_writer_append_value(db, &avro_value);
        if (rval) {
            fprintf(stderr, "Unable to write datum: %d: %s\n", rval, avro_strerror());
        }

        // Cleanup
        avro_value_reset(&avro_value);
        free(buffer);
        buffer = NULL;
        len = 0;
    }
    fprintf(stderr, "Finished serialization\n");

    avro_file_writer_flush(db);
    avro_file_writer_close(db);
    avro_value_decref(&avro_value);
    avro_value_iface_decref(iface);

    return 1;
}

// Returns - how many tokens to advance
int avro_record_value_fill(avro_value_t *avro_value, char *json) {
    int num_keys, rval;

    jsmntok_t *tokens;
    jsmntok_t *curtok;

    char *key, *value, *avro_keyname;
    avro_value_t avro_field, avro_subfield, avro_subsubfield;
    json_typeid type;

    // avro_value_iface_t *iface = avro_generic_class_from_schema(avro_schema_string());
    // avro_generic_value_new(iface, &avro_subsubfield);

    int arr_len;
    size_t index; // Index of field; just for debugging
    const char *fname;

    // Start code
    tokens = jsmn_tokenize(json);
    curtok = tokens;
    assert(curtok->type == JSMN_OBJECT);
    num_keys = curtok->size;
    ++curtok;
    for (int i = 0; i < num_keys; ++i) {
        key = jsmntok_to_str(curtok, json);
        ++curtok;

        type = jsmn_get_type(curtok, json);
        value = jsmntok_to_str(curtok, json);
        avro_keyname = to_avro_keyname(key, type, value);
        avro_value_get_by_name(avro_value, avro_keyname, &avro_field, &index);

        // fprintf(stderr, "Got to assignment for key: %s, value: %s type:%d\n", avro_keyname, value, type);
        switch (type) {
            case STRING:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                if (!strcmp(avro_keyname, "sparse_str")) {
                    avro_value_append(&avro_subfield, &avro_subsubfield, &index);
                    rval = avro_value_set_string(&avro_subsubfield, value);
                    // avro_value_get_string(&avro_subsubfield, &fname, &index);
                    // fprintf(stderr, "str: %s\n", fname);
                    // avro_value_get_size(&avro_subfield, &index);
                    // fprintf(stderr, "Sparse_str: new size: %zu\n", index);
                } else {
                    rval = avro_value_set_string(&avro_subfield, value);
                }
                break;
            case INTEGER:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                rval = avro_value_set_int(&avro_subfield, atoi(value));
                break;
            case FLOAT:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                rval = avro_value_set_float(&avro_subfield, atof(value));
                break;
            case BOOLEAN:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                rval =
                  avro_value_set_boolean(&avro_subfield,
                                         !strcmp(value, "true") ? 1 : 0);
                break;
            case DOCUMENT:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                for (int j = 0; j < 2; ++j) {
                    free(key);
                    free(value);
                    key = jsmntok_to_str(++curtok, json);
                    avro_value_get_size(&avro_subfield, &index);
                    //fprintf(stderr, "Size: %ld\n", index);
                    if (!strcmp(key, "str")) {
                        value = jsmntok_to_str(++curtok, json);
                        // TODO: see if i got this right
                        avro_value_get_by_name(&avro_subfield, "str_str", &avro_subsubfield, &index);
                        avro_value_set_string(&avro_subsubfield, value);
                    } else if (!strcmp(key, "num")) {
                        value = jsmntok_to_str(++curtok, json);
                        avro_value_get_by_name(&avro_subfield, "num_int", &avro_subsubfield, &index);
                        avro_value_set_int(&avro_subsubfield, atoi(value));
                    }
                }
                break;
            case ARRAY:
                rval = avro_value_set_branch(&avro_field, 0, &avro_subfield);
                arr_len = curtok->size;
                for (int j = 0; j < arr_len; ++j) {
                    // FIXME: I know that it's going to be a string, so I'm
                    // cheating and saving some code
                    avro_value_set_string(&avro_subsubfield,
                                          jsmntok_to_str(curtok + 1 + j, json));
                    avro_value_append(&avro_subfield, &avro_subsubfield, NULL);
                }
                curtok += arr_len;
            case NONE:
            default:
                break;
        }
        // fprintf(stderr, "after switch\n");

        if (rval) {
            fprintf(stderr, "Unable to set %s\n", avro_keyname);
            return rval;
        }

        free(key);
        free(value);
        free(avro_keyname);
        // fprintf(stderr, "on to next token\n");

        ++curtok;
    }

    return curtok - tokens;
}

char *to_avro_keyname(char *key, json_typeid type, char *value) {
    char *pg_type;
    char *avro_keyname;

    pg_type = get_pg_type(type, value);
    if (type == ARRAY) {
        pg_type = strtok(pg_type, "[");
    } else if (strcmp(key, "sparse_987") && strstr(key, "sparse")) {
        key = "sparse";
    }
    avro_keyname = malloc(sizeof(key) + 1 + sizeof(pg_type) + 1);
    sprintf(avro_keyname, "%s_%s", key, pg_type);

    return avro_keyname;
}
