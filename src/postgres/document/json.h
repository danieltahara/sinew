#include "lib/jsmn/jsmn.h"

typedef enum { STRING = 1,
               INTEGER,
               FLOAT,
               BOOLEAN,
               DOCUMENT,
               ARRAY,
               NONE
             } json_typeid;

#define STRING_TYPE "text"
#define INTEGER_TYPE "bigint"
#define FLOAT_TYPE "double precision"
#define BOOLEAN_TYPE "boolean"
#define DOCUMENT_TYPE "document"
#define ARRAY_TYPE "[]" /* The only non-terminal type we have */

/* JSMN type conversion functions */
json_typeid jsmn_primitive_get_type(char *value_str);
json_typeid jsmn_get_type(jsmntok_t* tok, char *json);

/* JSMN parsing routines */
char *jsmntok_to_str(jsmntok_t *tok, char *json);
jsmntok_t *jsmn_tokenize(char *json);

/* JSON type conversion function */
json_typeid get_json_type(const char *pg_type);
char *get_pg_type(json_typeid type, char *value);
