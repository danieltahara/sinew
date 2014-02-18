#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "document.h"
#include "schema.h"
#include "../json.h"

char dbname[] = "sinew_test.db";
char schemafname[] = "sinew_test.schema";
const char projected_keyname[] = "sparse_987";
const char projected_typename[] = STRING_TYPE;
const char *multiple_projected_keyname[] = { "sparse_987", "str1", "dyn1", "sparse_567", "str2",
                                              "sparse_123", "sparse_234", "sparse_345", "sparse_456", "sparse_789" };
const char *multiple_projected_typename[] = { "str", "str", "str", "str", "str",
                                              "str", "str", "str", "str", "str" };
const int num_projected_keys = 10;

int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_projection(FILE *outfile);
int test_multiple_projection(FILE *outfile);
char *extract_key(char *binary, const char* key, const char *type);

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename, *multiple_extract_outfilename;
    FILE *infile, *outfile, *extract_outfile, *multiple_extract_outfile;
    clock_t start, diff;
    int msec;

    infilename = argv[1];
    infile = fopen(infilename, "r");
    outfilename = argv[2];
    outfile = fopen(outfilename, "w");
    extract_outfilename = argv[3];
    extract_outfile = fopen(extract_outfilename, "w");
    multiple_extract_outfilename = argv[4];
    multiple_extract_outfile = fopen(multiple_extract_outfilename, "w");

    // Measures CPU Time
    // TODO: DRY
    start = clock();
    if (!test_serialize(infile)) {
        exit(EXIT_FAILURE);
    }
    fprintf(stderr, "%s\n", "made it here");
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Serialize: %d ms\n", msec);
    fflush(stdout);

    start = clock();
    if (!test_deserialize(outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Deserialize: %d ms\n", msec);
    fflush(stdout);

    start = clock();
    if (!test_projection(extract_outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Extract: %d ms\n", msec);
    fflush(stdout);

    start = clock();
    if (!test_multiple_projection(multiple_extract_outfile)) {
        exit(EXIT_FAILURE);
    }
    diff = clock() - start;
    msec = diff * 1000 / CLOCKS_PER_SEC;
    printf("Multiple Extract: %d ms\n", msec);
    fflush(stdout);

    fclose(infile);
    fclose(outfile);
    fclose(extract_outfile);
}

/* Read all the records and print them */
int test_deserialize(FILE *outfile) {
    FILE *dbfile, *schemafile;
    size_t binsize;
    char *binary, *json;

    dbfile = fopen(dbname, "r");
    schemafile = fopen(schemafname, "r");

    read_schema(schemafile);
    fprintf(stderr, "read schema\n");

    fread(&binsize, sizeof(binsize), 1, dbfile);
    while (!feof(dbfile)) {
        binary = malloc(binsize);
        fread(binary, binsize, 1, dbfile);

        // fprintf(stderr, "read object\n");
        json = binary_document_to_string(binary);
        // fprintf(stderr, "converted to json\n");
        fprintf(outfile, "%s\n", json);
        fflush(outfile);

        free(json);
        free(binary);
        fread(&binsize, sizeof(binsize), 1, dbfile);
    }

    fclose(schemafile);

    return 1;
}

int test_multiple_projection(FILE *outfile) {
    FILE *dbfile, *schemafile;
    size_t binsize;
    char *binary, *value;

    dbfile = fopen(dbname, "r");
    schemafile = fopen(schemafname, "r");

    read_schema(schemafile);
    fprintf(stderr, "read schema\n");

    fread(&binsize, sizeof(binsize), 1, dbfile);
    while (!feof(dbfile)) {
        char buffer[1000];
        sprintf(buffer, "");

        binary = malloc(binsize);
        fread(binary, binsize, 1, dbfile);
        for (int i = 0; i < num_projected_keys; ++i) {
            value = extract_key(binary, multiple_projected_keyname[i], multiple_projected_typename[i]);
            if (value) {
                sprintf(buffer, "%s%s,", buffer, value);
                free(value);
            } else {
                sprintf(buffer, "%s%s, ", buffer, "");
            }
        }
        fprintf(outfile, "%s\n", buffer);
        free(binary);

        fread(&binsize, sizeof(binsize), 1, dbfile);
    }

    fclose(schemafile);

    return 1;
}

int test_projection(FILE *outfile) {
    FILE *dbfile, *schemafile;
    size_t binsize;
    char *binary, *value;

    dbfile = fopen(dbname, "r");
    schemafile = fopen(schemafname, "r");

    read_schema(schemafile);
    fprintf(stderr, "read schema\n");

    fread(&binsize, sizeof(binsize), 1, dbfile);
    while (!feof(dbfile)) {
        binary = malloc(binsize);
        fread(binary, binsize, 1, dbfile);

        value = extract_key(binary, projected_keyname, projected_typename);
        if (value) {
            fprintf(outfile, "%s\n", value);
            free(value);
        }

        free(binary);

        fread(&binsize, sizeof(binsize), 1, dbfile);
    }

    fclose(schemafile);

    return 1;
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer, *binary;
    size_t len, read;
    size_t binsize; // NOTE: Must read in size_t bytes
    FILE *dbfile, *schemafile;

    dbfile = fopen(dbname, "w");
    schemafile = fopen(schemafname, "w");

    buffer = NULL;
    len = 0;
    while ((read = getline(&buffer, &len, infile)) != -1) {
        binsize = document_to_binary(buffer, &binary);
        fwrite(&binsize, sizeof(binsize), 1, dbfile);
        fwrite(binary, binsize, 1, dbfile);

        // Cleanup
        free(buffer);
        free(binary);
        buffer = NULL;
        len = 0;
    }

    dump_schema(schemafile);

    fclose(dbfile);

    return 1;
}

char *extract_key(char *binary, const char* key, const char *type) {
    char *value;
    char *attr_listing;
    size_t attr_id;
    int natts, buffpos, pos, len;
    int offstart, offend;

    attr_id = get_attribute_id(key, type);
    // fprintf(stderr, "Got attr id: %ld for: %s %s\n", attr_id, projected_keyname,
    //     projected_typename);

    memcpy(&natts, binary, sizeof(int));
    buffpos = sizeof(int);
    attr_listing = NULL;
    attr_listing = bsearch(&attr_id,
                           binary + buffpos,
                           natts,
                           sizeof(int),
                           int_comparator);
    if (attr_listing) {
        pos = (attr_listing - buffpos - binary) / sizeof(int);
        // fprintf(stderr, "attr_listing: %p\n", attr_listing);
        // fprintf(stderr, "binary: %p\n", binary);
        // fprintf(stderr, "Position: %d\n", pos);
        buffpos += natts * sizeof(int);
        memcpy(&offstart, binary + buffpos + pos * sizeof(int), sizeof(int));
        memcpy(&offend, binary + buffpos + (pos + 1) * sizeof(int), sizeof(int));
        len = offend - offstart;
        // fprintf(stderr, "Len: %d\n", len);

        value = malloc(len + 1);
        memcpy(value, binary + offstart, len);
        value[len] = '\0';
        return value;
    } else {
        return NULL;
    }
}
