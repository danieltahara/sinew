-- Copyright Hadapt, Inc. 2013
-- All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION json_fet" to load this file. \quit

CREATE OR REPLACE FUNCTION
json_get_int(cstring, cstring)
RETURNS integer
AS 'MODULE_PATHNAME','solr_get'
LANGUAGE C IMMUTABLE STRICT

CREATE OR REPLACE FUNCTION
json_get_float(cstring, cstring)
RETURNS double precision
AS 'MODULE_PATHNAME','solr_get'
LANGUAGE C IMMUTABLE STRICT

CREATE OR REPLACE FUNCTION
json_get_bool(cstring, cstring)
RETURNS boolean
AS 'MODULE_PATHNAME','solr_get'
LANGUAGE C IMMUTABLE STRICT

CREATE OR REPLACE FUNCTION
json_get_text(cstring, cstring)
RETURNS text
AS 'MODULE_PATHNAME','solr_get'
LANGUAGE C IMMUTABLE STRICT
