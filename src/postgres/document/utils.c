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

