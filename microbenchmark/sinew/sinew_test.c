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

int test_serialize(FILE* infile);
int test_deserialize(FILE *outfile);
int test_projection(FILE *outfile);

int main(int argc, char** argv) {
    char *infilename, *outfilename, *extract_outfilename;
    FILE *infile, *outfile, *extract_outfile;
    clock_t start, diff;
    int msec;

    infilename = argv[1];
    infile = fopen(infilename, "r");
    outfilename = argv[2];
    outfile = fopen(outfilename, "w");
    extract_outfilename = argv[3];
    extract_outfile = fopen(extract_outfilename, "w");

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

int test_projection(FILE *outfile) {
    FILE *dbfile, *schemafile;
    size_t attr_id, binsize;
    char *binary, *value;
    char *attr_listing;
    int natts, buffpos, pos, len;
    int offstart, offend;

    dbfile = fopen(dbname, "r");
    schemafile = fopen(schemafname, "r");

    read_schema(schemafile);
    fprintf(stderr, "read schema\n");

    fread(&binsize, sizeof(binsize), 1, dbfile);
    while (!feof(dbfile)) {
        binary = malloc(binsize);
        fread(binary, binsize, 1, dbfile);

        // NOTE: Hardcoded because I'm just testing one extraction
        attr_id = get_attribute_id(projected_keyname, projected_typename);
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
