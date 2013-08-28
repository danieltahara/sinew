-- Copyright Hadapt, Inc. 2013
-- All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION document_type" to load this file. \quit

-- NOTE: 'STRICT' tag signifies that if any input is null, output is also null

-- Serialization Functions

CREATE TYPE document;

CREATE OR REPLACE FUNCTION
string_to_document_datum(cstring)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_datum_to_string(document)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE document (
    INPUT = string_to_document_datum,
    OUTPUT = document_datum_to_string,
    INTERNALLENGTH = VARIABLE
);

CREATE SCHEMA IF NOT EXISTS document_schema;
CREATE TABLE IF NOT EXISTS document_schema._attributes(_id serial, key_name text NOT NULL, key_type text NOT NULL);

-- Accessors

CREATE OR REPLACE FUNCTION
document_get(document, cstring, cstring)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_get_int(document, cstring)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_get_float(document, cstring)
RETURNS double precision
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_get_bool(document, cstring)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_get_text(document, cstring)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_get_doc(document, cstring)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- Delete

CREATE OR REPLACE FUNCTION
document_delete(document, cstring, cstring)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- Put

CREATE OR REPLACE FUNCTION
document_put(document, cstring, cstring, cstring)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_put_int(document, cstring, bigint)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_put_float(document, cstring, double precision)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_put_bool(document, cstring, boolean)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_put_text(document, cstring, cstring)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
document_put_doc(document, cstring, document)
RETURNS document
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;
