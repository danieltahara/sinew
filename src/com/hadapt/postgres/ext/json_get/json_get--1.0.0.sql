-- Copyright Hadapt, Inc. 2013
-- All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION json_get" to load this file. \quit

CREATE OR REPLACE FUNCTION
json_get_int(text, cstring)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_get_float(text, cstring)
RETURNS double precision
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_get_bool(text, cstring)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION
json_get_text(text, cstring)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;
