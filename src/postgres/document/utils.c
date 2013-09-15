#include <assert.h>

#include <postgres.h>
#include "utils.h"

int
int_comparator(const void *v1, const void *v2)
{
    int i1, i2;

    i1 = *(int*)v1;
    i2 = *(int*)v2;

    if (i1 < i2)
    {
        return -1;
    }
    else if (i1 == i2)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

int
intref_comparator(const void *v1, const void *v2)
{
    int *i1, *i2;

    i1 = *(int**)v1;
    i2 = *(int**)v2;

    return int_comparator((void*)i1, (void*)i2);
}

char *
pstrndup(const char *str, int len)
{
    char *retval;

    retval = palloc0(len + 1);
    strncpy(retval, str, len);
    retval[len] = '\0';
    return retval;
}

int
parse_attr_path(char *attr_path, char ***path, char **path_arr_index_map)
{
    char *attr_path_copy;
    int path_depth;
    int path_max_depth;
    char *tok_start;
    int i;

    assert(attr_path);
    if (strlen(attr_path) == 0)
    {
        return 0;
    }

    /* This is a case where I'm not going to pfree and let PG manage it */
    attr_path_copy = pstrndup(attr_path, strlen(attr_path));
    path_depth = 0;
    path_max_depth = 8;
    *path = palloc0(path_max_depth * sizeof(char*));
    // elog(WARNING, "after initialization");

    tok_start = strtok(attr_path_copy, ".[");
    while (tok_start != NULL)
    {
        if (*tok_start == '\0')
        {
            elog(ERROR,
                 "parse_path: trailing symbol in attribute path - %s",
                 attr_path);
        }
        (*path)[path_depth++] = tok_start;
        if (path_depth >= path_max_depth)
        {
            path_max_depth *= 2;
            *path = repalloc(*path, path_max_depth * sizeof(char*));
        }
        tok_start = strtok(NULL, ".[");
    }

    *path_arr_index_map = palloc0(path_depth);
    for (i = 0; i < path_depth; i++)
    {
        char *path_elt;
        int len;

        path_elt = (*path)[i];
        len = strlen(path_elt);
        if (len > 0)
        {
            if (path_elt[len-1] == ']')
            {
                char *pend;
                int index;

                (*path_arr_index_map)[i] = true;
                path_elt[len-1] = '\0';

                pend = NULL;
                index = strtol(path_elt, &pend, 10);
                if (index == 0 && pend == NULL)
                {
                    elog(ERROR,
                         "parse_path: array index not an int - %s",
                         attr_path);
                }
                else if (index < 0)
                {
                    elog(ERROR,
                         "parse_path: cannot have neg. array index - %s",
                         attr_path);
                }
                else if (*pend != '\0')
                {
                    do
                    {
                        if (!isspace(*pend))
                        {
                            elog(ERROR,
                                 "parse_path: invalid path - %s",
                                 attr_path);
                        }
                        ++pend;
                    } while (*pend != '\0');
                }
            }
        }
        else
        {
            elog(ERROR, "parse_path: invalid attribute path - %s", attr_path);
        }
    }

    return path_depth;
}
