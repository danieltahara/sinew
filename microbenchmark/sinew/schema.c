#include <assert.h>

#include "../json.h"
#include "hash_table.h"
#include "schema.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Globals */
static int num_keys = 0; /* Length of key_names and key_types */
static char **key_names = NULL;
static char **key_types = NULL;

static table_t *attr_table = NULL;

/* NOTE: Assumes that key_names, key_types are valid */
void
get_attr(int id, char **key_name_ref, char **key_type_ref)
{
    // elog(WARNING, "get attr info: %d", id);
    if (id > num_keys)
    {
         *key_name_ref = NULL;
         *key_type_ref = NULL;
    }
    else
    {
         *key_name_ref = strndup(key_names[id], strlen(key_names[id]));
         *key_type_ref = strndup(key_types[id], strlen(key_types[id]));
    }
    // elog(WARNING, "finished");
}

/* Assumes it's there */
int
get_attribute_id(const char *keyname, const char *typename)
{
    int attr_id;
    char *attr;

    attr = calloc(strlen(keyname) + strlen(typename) + 2, 1);
    sprintf(attr, "%s %s", keyname, typename);

    if (attr_table) {
        attr_id = get(attr_table, attr);
        return attr_id;
    } else {
        fprintf(stderr, "Attr table not initialized");
        return -1;
    }
}

int
add_attribute(const char *keyname, const char *typename)
{
    char *attr;

    attr = calloc(strlen(keyname) + strlen(typename) + 2, 1);
    sprintf(attr, "%s %s", keyname, typename);

    if (!attr_table) {
        attr_table = make_table();
    }

    put(attr_table, attr, num_keys++);
    key_names = realloc(key_names, num_keys * sizeof(char**));
    key_types = realloc(key_types, num_keys * sizeof(char**));
    key_names[num_keys - 1] = strndup(keyname, strlen(keyname));
    key_types[num_keys - 1] = strndup(typename, strlen(typename));

    return num_keys - 1;
}

void
dump_schema(FILE* outfile)
{
    char *keyname, *typename, *attr;
    int i;

    fwrite(&num_keys, sizeof(int), 1, outfile);
    for (i = 0; i < num_keys; ++i) {
        keyname = key_names[i];
        typename = key_types[i];

        attr = calloc(strlen(keyname) + strlen(typename) + 3, 1);
        sprintf(attr, "%s %s\n", keyname, typename);

        fwrite(attr, 1, strlen(attr), outfile);
        free(attr);
    }
    fflush(outfile);
}

int
read_schema(FILE* infile)
{
    char *keyname, *typename, *attr;
    size_t i, len;

    if (attr_table) {
        destroy_table(attr_table);
    }
    attr_table = make_table();

    fread(&num_keys, sizeof(int), 1, infile);
    key_names = calloc(num_keys, sizeof(char**));
    key_types = calloc(num_keys, sizeof(char**));

    attr = NULL;
    len = 0;
    fprintf(stderr, "Num keys: %d\n", num_keys);
    for (i = 0; i < num_keys; ++i) {
        getline(&attr, &len, infile);

        keyname = strtok(attr, " ");
        typename = strtok(NULL, "\n");
        // Add to array
        key_names[i] = strndup(keyname, strlen(keyname));
        key_types[i] = strndup(typename, strlen(typename));
        // Add to hash table
        // FIXME: this is a bit hacky but w/e
        attr = malloc(strlen(keyname) + strlen(typename) + 2);
        sprintf(attr, "%s %s", keyname, typename);

        put(attr_table, attr, i);

        free(attr);
        attr = NULL;
        len = 0;
    }

    return 1;
}
