#include "nobench.pb.h"

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstdio>

char dbname[] = "protobuf_test.db";
char projected_key[] = "sparse_987_str";

int main(int argc, char** argv) {
    char *filename;
    FILE *infile, *outfile;
    int rval; // Function status codes
    clock_t start, diff;
    int msec;

    // Verify that the version of the library that we linked against is
    //   // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    infilename = argv[0];
    infile = fopen(infilename, "r");
    outfilename = argv[1];
    outfile = fopen(outfilename, "w");
    extract_outfilename = argv[2];
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
    if (test_extract(extract_outfile)) {
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
    NoBench nb;
    char *json;
    fstream input(dbname, ios::in | ios::binary);

    while (nb.ParseFromIstream(&input)) {
        json = calloc(10000, 1);
        strcat(json, "{");
        strcat(json, "\"%s\":\"%s\"", "str1", nb.get_str1_str());
        strcat(json, "\"%s\":\"%s\"", "str2", nb.get_str2_str());
        strcat(json, "\"%s\":%d", "num", nb.get_num_int());
        strcat(json, "\"%s\":%d", "bool", nb.get_bool_bool(());
        strcat(json, "\"%s\":\"%s\"", "dyn1", nb.get_dyn1_str());
        strcat(json, "\"%s\":\"%s\"", "dyn2", nb.get_dyn2_str());
        // Junk just for the sake of I/O
        strcat(json, "\"%s\":\"%s\"", "str1", nb.get_str1_str());
        strcat(json, "\"%s\":\"%s\"", "str2", nb.get_str2_str());
        strcat(json, "\"%s\":%d", "num", nb.get_num_int());
        strcat(json, "\"%s\":%d", "bool", nb.get_bool_bool(());
        strcat(json, "\"%s\":\"%s\"", "dyn1", nb.get_dyn1_str());
        strcat(json, "\"%s\":\"%s\"", "dyn2", nb.get_dyn2_str());
        strcat(json, "\"%s\":\"%s\"", "str1", nb.get_str1_str());
        strcat(json, "\"%s\":\"%s\"", "str2", nb.get_str2_str());
        strcat(json, "\"%s\":%d", "num", nb.get_num_int());
        strcat(json, "\"%s\":%d", "bool", nb.get_bool_bool(());
        strcat(json, "\"%s\":\"%s\"", "dyn1", nb.get_dyn1_str());
        strcat(json, "\"%s\":\"%s\"", "dyn2", nb.get_dyn2_str());
        fprintf(outfile, "%s\n", json);
        free(json);
    }
}

/* See: http://dcreager.github.io/avro-examples/resolved-writer.html */
int test_projection(FILE *outfile) {
    NoBench nb;
    fstream input(dbname, ios::in | ios::binary);

    while (nb.ParseFromIstream(&input)) {
        fprintf(outfile, "%s\n", nb.get_sparse_987_str());
    }
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len;
    fstream output(dbname, ios::out | ios::trunc | ios::binary);

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        NoBench nb;

        protobuf_fill(&nb, json);
        if (!nb.SerializeToOstream(&output)) {
            cerr << "Failed to write datum." << endl;
            return -1;
        }

        free(buffer);
        buffer = NULL;
        len = 0;
    }
}

// Returns - how many tokens to advance
int protobuf_fill(NoBench *protobuf, char *json) {
    int num_keys;

    jsmntok_t *tokens;
    jsmntok_t curtok;

    char *key, *value, *protobuf_keyname;
    json_typeid type;

    bool valbool;
    int arr_len;
    json_typeid arr_type;

    // Start code

    assert(curtok->type == JSMN_OBJECT);
    num_keys = curtok->size;

    tokens = jsmn_tokenize(buffer);
    curtok = tokens;
    ++curtok;
    for (int i = 0; i < num_keys; ++i) {
        key = jsmntok_to_str(curtok, json);
        ++curtok;

        type = jsmn_get_type(curtok, json);
        value = jsmntok_to_str(curtok, json);

        switch (type) {
            case STRING:
                if (!strcmp(key, "sparse_987")) {
                    protobuf->set_sparse_987_str(value);
                } else if (!strcmp(key, "str1")) {
                    protobuf->set_str1_str(value);
                } else if (!strcmp(key, "str2")) {
                    protobuf->set_str2_str(value);
                } else if (!strcmp(key, "dyn1")) {
                    protobuf->set_dyn1_str(value);
                } else if (!strcmp(key, "dyn2")) {
                    protobuf->set_dyn2_str(value);
                } else if (strstr(key, "sparse")) {
                    protobuf->add_sparse_str(value);
                }
                break;
            case INTEGER:
                if (!strcmp(key, "num")) {
                    protobuf->set_num_int(atoi(value));
                } else if (!strcmp(key, "dyn1")) {
                    protobuf->set_dyn1_int(atoi(value));
                } else if (!strcmp(key, "dyn2")) {
                    protobuf->set_dyn2_int(atoi(value));
                } else if (!strcmp(key, "thousandth")) {
                    protobuf->set_thousandth(atoi(value));
                }
                break;
            case FLOAT:
                break;
            case BOOLEAN:
                valbool = !strcmp(value, "true") ? true : false;
                if (!strcmp(key, "bool")) {
                    protobuf->set_bool_bool(valbool);
                } else if (!strcmp(key, "dyn2")) {
                    protobuf->set_dyn2_bool(valbool);
                }
                break;
            case DOCUMENT:
                for (j = 0; j < 2; ++j) {
                    free(key);
                    free(value);
                    key = jsmntok_to_str(++curtok, json);
                    if (!strcmp(key, "str")) {
                        value = jsmntok_to_str(++curtok, json);
                        protobuf->set_nested_obj_str_str(value);
                    } else if (!strcmp(key, "num")) {
                        value = jsmntok_to_str(++curtok, json);
                        protobuf->set_nested_obj_num_int(atoi(value));
                    }
                }
                break;
            case ARRAY:
                arr_len = curtok->size;
                for (int j = 0; j < arr_len; ++j) {
                    free(value);
                    value = jsmntok_to_str(++curtok, json);
                    protobuf->add_nested_arr_str(value);
                }
            case NONE:
            default:
        }

        free(key);
        free(value);

        ++curtok;
    }

    return curtok - tokens;
}
