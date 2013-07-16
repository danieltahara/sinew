#include "postgres.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "pg_type.h"

#include <ctype.h>
#include <stdio.h>

#include "lib/jsmn.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define NULLOID (-1); /* Mock, invalid postgres object id for NULL; used in
                         infer_type */

extern Datum doc_insert(PG_FUNCTION_ARGS);

static int
infer_type(char *value)
{
    assert(value);

    int len = strlen(value);
    assert(len > 0); // Empty string has length 2, integer has length >= 1

    char first_char = value[0];
    if (first_char == '"')
    {
        return TEXTOID;
    }
    else if (isdigit(first_char))
    {
        if (strchr(value, '.'))
        {
            return FLOAT8OID;
        }
        else
        {
            return INT8OID; // NOTE: Assumes max 8 bits
        }
    }
    else if (!strcmp(value, "true") || !strcmp(value, "false"))
    {
        return BOOLOID;
    }
    else if (!strcmp(value, "null"))
    {
        return NULLOID;
    }
    else if (first_char == '{')
    {
        return JSONOID;
    }
    else if (first_char == '[')
    {
        // FIXME: find the first comma or close ]
        int elemEnd = strchr(value, ",");
        // return infer_type(char)
    }
    else
    {
        elog(ERROR, "doc_insert: Got invalid json: %s", value);
    }
}

static Datum
make_datum(char *value, int type)
{
    switch (type) {
    case INT8OID:
        return atoi(value);
    case FLOAT8OID:
        return atof(value);
    case BOOLOID:
        return DatumGetBool(!strcmp(value, "true"))
    default:
        return value;
    }
    //FIXME:
}

PG_FUNCTION_INFO_V1(doc_insert);

Datum
doc_insert(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc   tupdesc;
    HeapTuple   rettuple;
    bool        isnull;
    int         ret, i, natts;
    char       *json;

    /* Make sure it's called as a trigger */
    if (!CALLED_AS_TRIGGER(fcinfo))
    {
        elog(ERROR, "doc_insert: not called by trigger manager");
    }

    /* Get schema for relation */
    tupdesc = trigdata->tg_relation->rd_att;
    natts = tupdesc->natts;

     /* Connect to SPI manager */
    if ((ret = SPI_connect()) < 0)
    {
        elog(ERROR, "doc_insert: SPI_connect returned %d", ret);
    }

    /* NOTE: For now, we assume only one json column per table */
    for (int i = 1; i <= natts; i++)
    {
         if (SPI_gettypeid(tupdesc, i) == JSONOID)
         {
            json = SPI_getvalue(trigdata->tg_trigtuple, tupdesc, i); break;
         }
    }

    /* For each existing column, see if there exists a value in the json data */
    int ncols = 0;
    int colnum[natts];
    Datum values[natts];
    for (int i = 1; i <= tupdesc->natts; i++)
    {
        /* Skip a column for which a value is already defined */
        if (SPI_getValue(trigdata->tg_trigtuple, tupdesc, i) != NULL)
        {
            continue;
        }

        char *name = SPI_fname(tupdesc, i);
        char *command = fprintf("SELECT json_object_field('%s', '%s');", json, name);
        SPI_execute(command, true, 0);
        /* NOTE: for now, assume each key only appears once */
        char *value = DatumGetCString(SPI_getvalue(SPI_tuptable->vals[0],
                                                   SPI_tuptable->tupdesc,
                                                   1));
        if (value != NULL) {
            int type = infer_type(value);
            if (type != SPI_gettypeid(tupdesc, i))
            {
                /* If type == -1, just a null, so skip it */
                if (type > 0)
                {
                    /* TODO: insert that one into exceptions table */
                }
            }
            else
            {
                Datum[ncols] = make_datum(value, type);
                ++ncols;
            }
        }

    }
    char nulls[ncols];
    for (int i = 0; i < ncols; ++i)
    {
        nulls[i] = ' ';
    }
    rettuple = SPI_modifytuple(trigdata->tg_relation,
                               trigdata->tg_trigtuple,
                               ncols,
                               colnum,
                               values,
                               nulls);

    /* Cleanup */
    SPI_finish();

    return PointerGetDatum(rettuple);
}