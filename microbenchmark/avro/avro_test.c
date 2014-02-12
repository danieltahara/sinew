#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "avro.h"
#include "nobench_schema.h"
#include "json.h"

#ifdef DEFLATE_CODEC
#define NOBENCH_CODEC  "deflate"
#else
#define NOBENCH_CODEC  "null"
#endif
#endif

avro_schema_t nobench_schema;

int main(int argc, char** argv) {
    char *filename;
    FILE *infile; // Input JSON Data
    int rval; // Function status codes
    clock_t start, diff;
    int msec;

    // Parse schema into a schema data structure
    if (avro_schema_from_json_literal(NOBENCH_SCHEMA, &nobench_schema)) {
        fprintf(stderr, "Unable to parse person schema\n");
        exit(EXIT_FAILURE);
    }

    infilename = argv[0];
    infile = fopen(infilename, "r");
    outfilename = argv[1];
    keyname = argv[2];
    dbname = argv[3]; // TODO: remove

    // Measures CPU Time
    // TODO: DRY
    start = clock();
    if (test_serialize(infile, dbname)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Serialize: %d ms", msec);

    start = clock();
    if (test_deserialize(dbname)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Deserialize: %d ms", msec);

    start = clock();
    if (test_extract(keyname, dbname)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Extract: %d ms", msec);
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len;
    avro_file_writer_t db;
    avro_value_iface_t *iface;
    int rval;

    rval = avro_file_writer_create_with_codec(dbname,
                                              nobench_schema,
                                              &db,
                                              NOBENCH_CODEC,
                                              0);
    if (rval) {
        fprintf(stderr, "There was an error creating %s\n", dbname);
        fprintf(stderr, " error message: %s\n", avro_strerror());
        return rval;
    }

    iface = avro_generic_class_from_schema(nobench_schema);

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        jsmntok_t *tokens;
        jsmntok_t curtok;
        int num_keys;
        char *key, *value, *avro_keyname;
        json_typeid type;
        avro_value_t avro_value;
        avro_value_t avro_field;

        // Create a new record
        avro_generic_value_new(iface, &value);

        tokens = jsmn_tokenize(buffer);
        curtok = tokens;

        assert(curtok->type == JSMN_OBJECT);
        num_keys = curtok->size;
        ++curtok;
        for (int i = 0; i < num_keys; ++i) {
            key = jsmntok_to_str(curtok, json);
            ++curtok;

            type = jsmn_get_type(curtok, json);
            value = jsmntok_to_str(curtok, json);
            avro_keyname = to_avro_keyname(key, type);
            avro_value_get_by_name(&avro_value, avro_keyname, &avro_field, NULL);

            switch (type) {
                case STRING:
                    rval = avro_value_set_string(&avro_field, value);
                    break;
                case INTEGER:
                    rval = avro_value_set_int(&avro_field, atoi(value));
                    break;
                case FLOAT:
                    rval = avro_value_set_int(&avro_field, atof(value));
                    break;
                case BOOLEAN:
                    rval =
                      avro_value_set_boolean(&avro_field,
                                             !strcmp(value, "true") ? 1 : 0);
                    break;
                case DOCUMENT:
                    // TODO:
                    // num elts -> recursion
                case ARRAY:
                    // TODO:
                    // num elts -> just curtok++
                case NONE:
                default:
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

        rval = avro_file_writer_append(db, nobench);
        if (rval) {
            fprintf(stderr,
                    "Unable to write datum to memory buffer\nMessage: %s\n",
                    avro_strerror());
        }

        avro_value_decref(avro_value);
        free(buffer);
        buffer = NULL;
        len = 0;
    }
    avro_value_iface_decref(iface);
}
