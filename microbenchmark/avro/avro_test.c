#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "avro.h"
#include "nobench_schema.h"
#include "../json.h"

char dbname[] = "avro_test.db";
avro_schema_t nobench_schema;
avro_schema_t projected_schema;

char *to_avro_keyname(char *key, json_typeid type, char *value);
int avro_record_value_fill(avro_value_t *avro_value, char *json);
int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_project(FILE *outfile);

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename;
    FILE *infile, *outfile, *extract_outfile;
    int rval; // Function status codes
    clock_t start, diff;
    int msec;

    // Parse schema into a schema data structure
    if (avro_schema_from_json_literal(NOBENCH_SCHEMA, &nobench_schema)) {
        fprintf(stderr, "Unable to parse person schema\n");
        exit(EXIT_FAILURE);
    }
    if (avro_schema_from_json_literal(PROJECTED_SCHEMA, &projected_schema)) {
        fprintf(stderr, "Unable to parse person schema\n");
        exit(EXIT_FAILURE);
    }

    infilename = argv[1];
    infile = fopen(infilename, "r");
    outfilename = argv[2];
    outfile = fopen(outfilename, "w");
    extract_outfilename = argv[3];
    extract_outfile = fopen(extract_outfilename, "w");

    // Measures CPU Time
    // TODO: DRY
    start = clock();
    if (test_serialize(infile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Serialize: %d ms", msec);

    start = clock();
    if (test_deserialize(outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Deserialize: %d ms", msec);

    start = clock();
    if (test_project(extract_outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Extract: %d ms", msec);

    fclose(infile);
    fclose(outfile);
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

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len, read;
    avro_file_writer_t db;
    avro_value_iface_t *iface;
    int rval;

    rval = avro_file_writer_create(dbname, nobench_schema, &db);

    if (rval) {
        fprintf(stderr, "There was an error creating %s\n", dbname);
        fprintf(stderr, " error message: %s\n", avro_strerror());
        return rval;
    }

    iface = avro_generic_class_from_schema(nobench_schema);

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        avro_value_t avro_value;

        // Create a new record
        avro_generic_value_new(iface, &avro_value);
        avro_record_value_fill(&avro_value, buffer);

        // Write the record
        rval = avro_file_writer_append_value(db, &avro_value);
        if (rval) {
            fprintf(stderr,
                    "Unable to write datum to memory buffer\nMessage: %s\n",
                    avro_strerror());
        }

        // Cleanup
        avro_value_decref(&avro_value);
        free(buffer);
        buffer = NULL;
        len = 0;
    }

    avro_file_writer_flush(db);
    avro_value_iface_decref(iface);

    return 1;
}

// Returns - how many tokens to advance
int avro_record_value_fill(avro_value_t *avro_value, char *json) {
    int num_keys, rval;

    jsmntok_t *tokens;
    jsmntok_t *curtok;

    char *key, *value, *avro_keyname;
    json_typeid type;
    avro_value_t avro_field;

    int arr_len;
    avro_value_t avro_arr_element;
    json_typeid arr_type;

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
        avro_value_get_by_name(avro_value, avro_keyname, &avro_field, NULL);

        switch (type) {
            case STRING:
                rval = avro_value_set_string(&avro_field, value);
                break;
            case INTEGER:
                rval = avro_value_set_int(&avro_field, atoi(value));
                break;
            case FLOAT:
                rval = avro_value_set_float(&avro_field, atof(value));
                break;
            case BOOLEAN:
                rval =
                  avro_value_set_boolean(&avro_field,
                                         !strcmp(value, "true") ? 1 : 0);
                break;
            case DOCUMENT:
                curtok += avro_record_value_fill(&avro_field, value);
                break;
            case ARRAY:
                arr_len = curtok->size;
                arr_type = jsmn_get_type(curtok + 1, json);
                for (int j = 0; j < arr_len; ++j) {
                    // FIXME: I know that it's going to be a string, so I'm
                    // cheating and saving some code
                    avro_value_set_string(&avro_arr_element,
                                          jsmntok_to_str(curtok + 1 + j, json));
                    avro_value_append(&avro_field, &avro_arr_element, NULL);
                }
                curtok += arr_len;
            case NONE:
            default:
                break;
        }

        if (rval) {
            fprintf(stderr, "Unable to set %s", avro_keyname);
            return rval;
        }

        free(key);
        free(value);
        free(avro_keyname);

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
    }
    avro_keyname = malloc(sizeof(key) + 1 + sizeof(pg_type) + 1);
    sprintf(avro_keyname, "%s_%s", key, pg_type);

    return avro_keyname;
}
