-- Copyright Hadapt, Inc. 2013
-- All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION schema_analyzer" to load this file. \quit

-- Relies on document_type
CREATE EXTENSION IF NOT EXISTS document_type;

CREATE OR REPLACE FUNCTION analyze_document()
RETURNS trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION analyze_schema()
RETURNS trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;
