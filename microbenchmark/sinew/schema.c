#include <assert.h>

#include "lib/jsmn/jsmn.h"
#include "utils.h"
#include "json.h"
#include "hash_table.h"
#include "schema.h"

/* Globals */
static int num_keys = 0; /* Length of key_names and key_types */
static char **key_names = NULL;
static char **key_types = NULL;

static table_t *attr_table = NULL;

/* NOTE: Assumes that key_names, key_types are valid */
static void
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
    key_names = realloc(key_names, num_keys, sizeof(char**));
    key_types = realloc(key_names, num_keys, sizeof(char**));
    key_names[num_keys - 1] = strndup(keyname, strlen(keyname));
    key_types[num_keys - 1] = strndup(typename, strlen(typename));

}
