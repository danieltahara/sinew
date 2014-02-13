#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "document.h"
#include "json.h"

char *dbname = "sinew_test.db"
char *schemafname = "sinew_test.schema"

int main(int argc, char** argv) {
    char *filename;
    FILE *infile, *outfile;
    clock_t start, diff;
    int msec;

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
}

int test_projection(FILE *outfile) {
}

// NOTE: File must be \n terminated
int test_serialize(FILE* infile) {
    char *buffer;
    size_t len;
    size_t binsize;
    FILE *dbfile;

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

    // TODO: Write pairs to db schemafile

    fclose(dbfile);
}
