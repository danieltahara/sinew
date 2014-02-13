#include "json.h"

typedef struct {
    int        natts;
    char     **keys;
    json_typeid *types;
    char     **values;
} document;

void json_to_document(char *json, document *doc);
int array_to_binary(char *json_arr, char **outbuff_ref);
int document_to_binary(char *json, char **outbuff_ref);
int to_binary(json_typeid type, char *value, char **outbuff_ref);

/* TODO: should probably pass an outbuff ref in the same way as above */
/* NOTE: the reason I pass an outbuff ref above is because the document binary
 * can potentially have an x00 value, which would mess up attempts at using
 * strlen(str). Since the deserialization functions return json strings, they
 * do not face the same problem */
void binary_to_document(char *binary, document *doc);
char *binary_document_to_string(char *binary);
char *binary_array_to_string(char *binary);
char *binary_to_string(json_typeid type, char *binary, int datum_len);
