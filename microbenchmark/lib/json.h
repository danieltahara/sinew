#include "lib/jsmn/jsmn.h"

typedef enum { STRING = 1,
               INTEGER,
               FLOAT,
               BOOLEAN,
               DOCUMENT,
               ARRAY,
               NONE
             } json_typeid;

#define STRING_TYPE "str"
#define INTEGER_TYPE "int"
#define FLOAT_TYPE "double"
#define BOOLEAN_TYPE "bool"
#define DOCUMENT_TYPE "obj"
#define ARRAY_TYPE "[]" /* The only non-terminal type we have */
#define NULL_TYPE "null"

/* JSMN type conversion functions */
json_typeid jsmn_primitive_get_type(char *value_str);
json_typeid jsmn_get_type(jsmntok_t* tok, char *json);

/* JSMN parsing routines */
char *jsmntok_to_str(jsmntok_t *tok, char *json);
jsmntok_t *jsmn_tokenize(char *json);

/* JSON type conversion function */
json_typeid get_json_type(const char *pg_type);
char *get_pg_type(json_typeid type, char *value);
char *get_pg_type_for_path(char **path,
                           char *path_arr_index_map,
                           int depth,
                           char *base_type);
