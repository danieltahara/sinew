#include <stdio.h>

void get_attr(int id,
                   char **key_name_ref,
                   char **type_name_ref); /* TODO: better name */
int get_attribute_id(const char *key_name, const char *type_name);
int add_attribute(const char *key_name, const char *type_name);
void dump_schema(FILE* outfile);
int read_schema(FILE* infile);
