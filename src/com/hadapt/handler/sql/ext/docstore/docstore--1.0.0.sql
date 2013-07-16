CREATE TABLE IF NOT EXISTS information_schema.documents(
   key_name text, table_name text, column_name text,
   type text, materialized boolean, clean boolean, count int,
   UNIQUE (key_name, table_name));

CREATE TABLE IF NOT EXISTS information_schema.documents_exceptions(
   key_name text, table_column text, table_name text,
   type text, row_id int);
