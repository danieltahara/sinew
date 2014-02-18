#include <iostream>
#include <fstream>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "nobench.pb.h"
#include "json.h"

char dbname[] = "protobuf_test.db";
char projected_key[] = "sparse_987_str";

int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_project(FILE *outfile);
int protobuf_fill(Database::NoBench *protobuf, char *json);

using namespace std;

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename;
    FILE *infile, *outfile, *extract_outfile;
    int rval; // Function status codes
    clock_t start, diff;
    int msec;

    // Verify that the version of the library that we linked against is
    //   // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    infilename = argv[1];
    infile = fopen(infilename, "r");
    // Measures CPU Time
    // TODO: DRY
    start = clock();
    if (!test_serialize(infile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Serialize: %d ms", msec);
    fflush(stdout);
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
    fflush(stdout);
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
    fflush(stdout);
    fclose(extract_outfile);
}

/* Read all the records and print them */
int test_deserialize(FILE *outfile) {
    Database db;
    Database::NoBench *nb;
    char *json;
    char buffer[10000];
    fstream input(dbname, ios::in | ios::binary);

    fprintf(stderr, "In test deser\n");

    db.ParseFromIstream(&input);
    for (int i = 0; i < db.nb_size(); ++i) {
        nb = db.mutable_nb(i);

        json = (char*)calloc(10000, 1);
        strcat(json, "{");
        sprintf(buffer, "\"ID\":%d", i);
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "str1", nb->str1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "str2", nb->str1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\": %lld", "num", nb->num_int());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":%d", "bool", nb->bool_bool());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "dyn1", nb->dyn1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "dyn2", nb->dyn2_str().c_str());
        strcat(json, buffer);
        // /Junk just for the sake of I/O
        sprintf(buffer, "\"%s\":\"%s\"", "str1", nb->str1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "str2", nb->str1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\": %lld", "num", nb->num_int());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":%d", "bool", nb->bool_bool());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "dyn1", nb->dyn1_str().c_str());
        strcat(json, buffer);
        sprintf(buffer, "\"%s\":\"%s\"", "dyn2", nb->dyn2_str().c_str());
        strcat(json, buffer);
        // \junk

        fprintf(outfile, "%s\n", json);
        free(json);
    }

    return 1;
}

/* See: http://dcreager.github.io/avro-examples/resolved-writer.html */
int test_project(FILE *outfile) {
    Database db;
    Database::NoBench *nb;
    fstream input(dbname, ios::in | ios::binary);

    db.ParseFromIstream(&input);
    for (int i = 0; i < db.nb_size(); ++i) {
        nb = db.mutable_nb(i);
        fprintf(outfile, "%s\n", nb->sparse_987_str().c_str());
    }

    return 1;
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len, read;
    fstream output(dbname, ios::out | ios::trunc | ios::binary);
    Database db;

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        Database::NoBench *nb;

        nb = db.add_nb();
        protobuf_fill(nb, buffer);

        free(buffer);
        buffer = NULL;
        len = 0;
    }

    if (!db.SerializeToOstream(&output)) {
        cerr << "Failed to write datum." << endl;
        return -1;
    }
    fprintf(stderr, "Finished serialize\n");

    return 1;
}

// Returns - how many tokens to advance
int protobuf_fill(Database::NoBench *protobuf, char *json) {
    int num_keys;

    jsmntok_t *tokens;
    jsmntok_t *curtok;

    char *key, *value, *protobuf_keyname;
    json_typeid type;

    bool valbool;
    int arr_len;
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
        // fprintf(stderr, "Got to assignment for key: %s, value: %s type:%d\n", key, value, type);

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
                for (int j = 0; j < 2; ++j) {
                    free(key);
                    free(value);
                    key = jsmntok_to_str(++curtok, json);
                    if (!strcmp(key, "str")) {
                        value = jsmntok_to_str(++curtok, json);
                        protobuf->mutable_nested_obj_obj()->set_str_str(value);
                    } else if (!strcmp(key, "num")) {
                        value = jsmntok_to_str(++curtok, json);
                        protobuf->mutable_nested_obj_obj()->set_num_int(atoi(value));
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
                break;
        }

        free(key);
        free(value);

        ++curtok;
    }

    return curtok - tokens;
}
