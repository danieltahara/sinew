CREATE TABLE IF NOT EXISTS information_schema.documents(
    key_name text, table_name text, column_name text,
    type text, materialized boolean, clean boolean, count int,
    UNIQUE (key_name, table_name));

CREATE TABLE IF NOT EXISTS information_schema.documents_exceptions(
    key_name text, table_column text, table_name text,
    type text, row_id int);

CREATE FUNCTION doc_insert() RETURNS trigger
    AS '$libdir/doc_insert.so'
    LANGUAGE C;

CREATE OR REPLACE FUNCTION configure_document_store(table_name text) AS $$
    CREATE TRIGGER tinsert BEFORE INSERT OR UPDATE OR DELETE ON text::relname -- FIXME?
        FOR EACH ROW EXECUTE PROCEDURE doc_insert();
$$ LANGUAGE PLPGSQL
