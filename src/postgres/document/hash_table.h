#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef struct element element_t;
typedef struct table table_t;

table_t *make_table(void);
void destroy_table(table_t *table);

//insert by copy; updates entry if exists, otherwise, adds
const element_t *put(table_t *ht, char *key, int val);
const int get(table_t *ht, char *key);

#endif
