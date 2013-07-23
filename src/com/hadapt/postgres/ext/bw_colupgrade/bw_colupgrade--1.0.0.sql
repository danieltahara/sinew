-- Copyright Hadapt, Inc. 2013
-- All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION json_get" to load this file. \quit

CREATE EXTENSION json_get;

CREATE FUNCTION bw_colupgrade_launch(void)
RETURNS pg_catalog.bool STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;