#define _GNU_SOURCE

#include <iostream>
#include <fstream>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nobench.pb.h"
#include "json.h"

char dbname[] = "protobuf_test.db";

int num_dbfiles = 0;
const int MAX_DB_SIZE = 100000;

int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_project(FILE *outfile);
int test_multiple_project(FILE *outfile);
int protobuf_fill(Database::NoBench *protobuf, char *json);

using namespace std;

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename, *multiple_extract_outfilename;
    FILE *infile, *outfile, *extract_outfile, *multiple_extract_outfile;
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
    printf("Serialize: %d ms\n", msec);
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
    printf("Deserialize: %d ms\n", msec);
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
    printf("Extract: %d ms\n", msec);
    fflush(stdout);
    fclose(extract_outfile);

    multiple_extract_outfilename = argv[4];
    multiple_extract_outfile = fopen(multiple_extract_outfilename, "w");
    start = clock();
    if (!test_multiple_project(multiple_extract_outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Multiple Extract: %d ms\n", msec);
    fflush(stdout);
    fclose(multiple_extract_outfile);
}

/* Read all the records and print them */
int test_deserialize(FILE *outfile) {
    Database db;
    Database::NoBench *nb;
    char *json;

    fprintf(stderr, "In test deser\n");

    for (int j = 0; j < num_dbfiles; ++j) {
        char dbname_full[200];
        sprintf(dbname_full, "%s_%d", dbname, j);
        fstream input(dbname_full, ios::in | ios::binary);

        db.ParseFromIstream(&input);
        // fprintf(stderr, "Parsing file %d of %d: name = %s db size = %d\n", j, num_dbfiles, dbname_full, db.nb_size());
        for (int i = 0; i < db.nb_size(); ++i) {
            nb = db.mutable_nb(i);

            json = (char*)calloc(10000, 1);
            sprintf(json, "{ \"ID\" : %d", i);
            for (int j = 0; j < 5; ++j) {
                // /Junk just for the sake of I/O
                sprintf(json, "%s, \"%s\":\"%s\"", json, "str1", nb->str1_str().c_str());
                sprintf(json, "%s, \"%s\":\"%s\"", json, "str2", nb->str2_str().c_str());
                sprintf(json, "%s, \"%s\":%ld", json, "num", nb->num_int());
                sprintf(json, "%s, \"%s\":%d", json, "bool", nb->bool_bool());
                sprintf(json, "%s, \"%s\":\"%s\"", json, "dyn1", nb->dyn1_str().c_str());
                sprintf(json, "%s, \"%s\":\"%s\"", json, "dyn2", nb->dyn2_str().c_str());
            }
            for (int j = 0; j < 1000; ++j) {
                // Junk for the sake of memory dereferences
                if (nb->has_dyn1_str()) {};
            }

            fprintf(outfile, "%s\n", json);
            free(json);
        }

        fflush(outfile);
        input.close();
        db.Clear();
    }

    return 1;
}

int test_project(FILE *outfile) {
    Database db;
    Database::NoBench *nb;

    // TODO: DRY with deser
    for (int j = 0; j < num_dbfiles; ++j) {
        char dbname_full[200];
        sprintf(dbname_full, "%s_%d", dbname, j);
        fstream input(dbname_full, ios::in | ios::binary);

        db.ParseFromIstream(&input);
        for (int i = 0; i < db.nb_size(); ++i) {
            nb = db.mutable_nb(i);
            if (nb->has_sparse_987_str()) {
                fprintf(outfile, "%s\n", nb->sparse_987_str().c_str());
            }
        }

        fflush(outfile);
        input.close();
        db.Clear();
    }

    return 1;
}

int test_multiple_project(FILE *outfile) {
    Database db;
    Database::NoBench *nb;

    // TODO: DRY with deser
    for (int j = 0; j < num_dbfiles; ++j) {
        char dbname_full[200];
        sprintf(dbname_full, "%s_%d", dbname, j);
        fstream input(dbname_full, ios::in | ios::binary);

        db.ParseFromIstream(&input);
        for (int i = 0; i < db.nb_size(); ++i) {
            char buffer[1000];
            sprintf(buffer, "");

            nb = db.mutable_nb(i);
            if (nb->has_sparse_987_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_987_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_123_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_123_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_234_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_234_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_345_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_345_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_456_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_456_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_567_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_567_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_sparse_789_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->sparse_789_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            if (nb->has_dyn1_str()) {
                sprintf(buffer, "%s%s, ", buffer, nb->dyn1_str().c_str());
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
            sprintf(buffer, "%s%s, ", buffer, nb->str1_str().c_str());
            sprintf(buffer, "%s%s, ", buffer, nb->str2_str().c_str());

            fprintf(outfile, "%s\n", buffer);
        }

        fflush(outfile);
        input.close();
        db.Clear();
    }

    return 1;
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len, read;
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

        if (db.nb_size() >= MAX_DB_SIZE) {
            char dbname_full[200];
            sprintf(dbname_full, "%s_%d", dbname, num_dbfiles++);
            fstream output(dbname_full, ios::out | ios::trunc | ios::binary);
            // fprintf(stderr, "Outputting file %d : db size = %d\n", num_dbfiles, db.nb_size());
            if (!db.SerializeToOstream(&output)) {
                cerr << "Failed to write datum." << endl;
                return -1;
            }
            output.close();
            db.Clear();
        }
    }

    if (db.nb_size() != 0) {
        char dbname_full[200];
        sprintf(dbname_full, "%s_%d", dbname, num_dbfiles++);
        fstream output(dbname_full, ios::out | ios::trunc | ios::binary);
        // fprintf(stderr, "Outputting file %d : db size = %d\n", num_dbfiles, db.nb_size());
        if (!db.SerializeToOstream(&output)) {
            cerr << "Failed to write datum." << endl;
            return -1;
        }
        output.close();
        db.Clear();
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
                } else if (!strcmp(key, "sparse_123")) {
                    protobuf->set_sparse_123_str(value);
                } else if (!strcmp(key, "sparse_234")) {
                    protobuf->set_sparse_234_str(value);
                } else if (!strcmp(key, "sparse_345")) {
                    protobuf->set_sparse_345_str(value);
                } else if (!strcmp(key, "sparse_456")) {
                    protobuf->set_sparse_456_str(value);
                } else if (!strcmp(key, "sparse_567")) {
                    protobuf->set_sparse_567_str(value);
                } else if (!strcmp(key, "sparse_789")) {
                    protobuf->set_sparse_789_str(value);
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
