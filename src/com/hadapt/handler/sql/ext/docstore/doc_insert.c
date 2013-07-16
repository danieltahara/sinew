#include "postgres.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "pg_type.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include "lib/jsmn.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef enum { STRING = 1, INTEGER, FLOAT, BOOLEAN, NULL } json_typeid;

extern Datum doc_insert(PG_FUNCTION_ARGS);

static char *
jsmntok_to_str(jsmntok_t *tok, char *json)
{
    char *retval;
    int   len;

    len = tok->end - tok->start;
    retval = palloc0(len + 1);
    strncpy(retval, json, len);

    return retval;
}

// TODO: JSMN_example license
static jsmntok_t *
jsmn_get(char *key, jsmntok_t *tokens)
{
    jsmntok_t *retval = NULL;

    typedef enum { START, KEY, VALUE, STOP } parse_state;
    parse_state state = START;
    for (size_t i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok = &tokens[i];

        /* Should never reach uninitialized tokens */
        assert(t->start != -1 && t->end != -1);

        switch (state)
        {
        case START:
            assert(curtok->type == JSMN_OBJECT);
            j += t->size;
            state = KEY;
            break;
        case KEY:
            assert(curtok->type == JSMN_STRING);
            if (!strcmp(key, string_for_tok(curtok, json)))
            {
                assert(j >= 1);
                retval = curtok + 1;
                state = STOP;
            }
            else
            {
                state = VALUE;
            }
            break;
        case VALUE:
            if (t->type == JSMN_ARRAY || t->type == JSMN_OBJECT)
            {
                i += t->size; /* Skip all of the tokens */
            }
            state = KEY;
            break;
        case STOP:
            return retval;
        }
    }
    return retval;
}

static char *
get_key_info(jsmntok_t *tokens, char *json)
{
    char *retval;
    char *cur_key, *cur_val;
    int   cur_size, max_size;

    cur_key = NULL;
    cur_val = NULL;
    cur_size = 0;
    max_size = 256;
    retval = palloc0(max_size);


    typedef enum { START, KEY } parse_state;
    parse_state state = START;
    for (size_t i = 0, j = 1; j > 0; ++i, --j)
    {
        jsmntok_t *curtok = &tokens[i];

        /* Should never reach uninitialized tokens */
        assert(t->start != -1 && t->end != -1);

        switch (state)
        {
        case START:
            assert(curtok->type == JSMN_OBJECT);
            j += t->size;
            state = KEY;
            break;
        case KEY:
            assert(curtok->type == JSMN_STRING);

            cur_key = jsmntok_to_str(curtok, json);

            curtok = &tokens[++i]; /* Advance to value token */
            --j;

            if (t->type == JSMN_ARRAY || t->type == JSMN_OBJECT)
            {
                cur_val = "text";
                i += t->size; /* Skip all of the tokens */
            }
            else
            {
                switch (infer_pg_type(curtok, json))
                {
                case STRING:
                    cur_val = "text";
                    break;
                case INTEGER:
                    cur_val = "integer";
                    break;
                case FLOAT:
                    cur_val = "real";
                    break;
                case BOOLEAN:
                    cur_val = "boolean";
                    break;
                case NULL:
                    pfree(cur_key);
                    continue;
                default:
                    elog(ERROR, "doc_insert: reached default case of get keys");
                }
            }

            int new_size = cur_size + strlen(cur_key) + 1 + strlen(cur_val);
            new_size += (cur_size == 0) ? 0 : 2; /* For comma delimiter */
            if (new_size + 1 >= max_size);
            {
                retval = prealloc(retval, 2 * new_size + 1);
            }
            sprintf(retval, "%s%s%s %s", retval, (cur_size == 0) ? "" : ", ", cur_key, cur_val);
            cur_size = new_size;
        }
    }
    return retval;
}

const static json_typeid
infer_pg_type(jsmntok_t *tok, char *json)
{
    if (!tok)
    {
        return NULL;
    }

    int start = tok->start;

    if (tok->type == JSMN_STRING) {
        return STRING;
    }
    else if (tok->type == JSMN_ARRAY || tok->type == JSMN_OBJECT) /* For now, treat a string */
    {
        return STRING;
    }
    else
    {
        switch(json[start]) {
        case 't': case 'f':
            return BOOLEAN;
        case 'n':
            return NULL;
        case '-':
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            char *val = jsmntok_to_str(tok, json);
            if (strchr(val, '.'))
            {
                pfree(val);
                return FLOAT;
            }
            else
            {
                pfree(val);
                return INTEGER;
            }
        default:
            elog(ERROR, "doc_insert: Got invalid json: %s", value);
        }
    }
}

/* NOTE: This is kind of limited/brittle as it stands */
static bool
can_create_from(json_typeid type, int dbType)
{
    switch (type)
    {
    case STRING:
        return (dbType == TEXTOID || dbType == CHAROID);
    case INTEGER:
        return (dbType == INT4OID || dbType == INT8OID);
    case FLOAT:
        return (dbType == FLOAT4OID || dbType == FLOAT8OID);
    case BOOLEAN:
        return (dbType == BOOLOID);
    case NULL:
        return false;
    case default:
        elog(ERROR, "doc_insert: reached default case of can_create_from");
    }
}

static Datum
make_datum(char *value, json_typeid type)
{
    assert(type != NULL);

    /* NOTE: Type checking must have been done, else ato* functions will bug out */
    switch (type)
    {
    case STRING:
        return CStringGetDatum(value);
    case INTEGER:
        return Int64GetDatum(atol(value));
    case FLOAT:
        return Float8GetDatum(atof(value));
    case BOOLEAN:
        return BoolGetDatum(!strcmp(value, "true"));
    case default:
        elog(ERROR, "doc_insert: reached default case of make_datum");
    }
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
            json = SPI_getvalue(trigdata->tg_trigtuple, tupdesc, i);
            break;
         }
    }
    jsmntok_t *tokens = json_tokenize(json);

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
        jsmntok_t value = jsmn_get(name, tokens);

        if (value != NULL) {
            const json_typeid tokType = infer_type(value);
            const int dbType = SPI_gettypeid(tupdesc, i);
            if (can_create_from(tokType, dbType))
            {
                /* Nulls do not constitute exceptions */
                if (tokType != NULLOID)
                {
                    /* FIXME: insert that one into exceptions table */
                    elog(WARNING, "doc_insert: reached type exception; did not add to table");
                }
            }
            else
            {
                Datum[ncols] = make_datum(jsmntok_to_str(value, json), dbType);
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

    /* Notify the listener about what keys were contained in the json */
    // TODO:
    char *key_info = get_key_info(tokens);
    char *payload = palloc0(strlen(key_info) + 50 + 1);
    sprintf(payload, "pg_notify('documents_schema_listener', %s)", key_info);
    SPI_execute(payload);
    // TODO: error handling?

    /* Cleanup */
    SPI_finish();
    pfree(key_info);
    pfree(payload);

    return PointerGetDatum(rettuple);
}