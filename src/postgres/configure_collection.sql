-- FIXME: call keys id => _id, data => _data, key_id => _id
CREATE OR REPLACE FUNCTION configure_document_store(tname text) RETURNS void AS $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS schema_analyzer;
  EXECUTE '
     ALTER TABLE ' || tname::regclass || ' ADD COLUMN id serial; 
     ALTER TABLE ' || tname::regclass || ' ADD COLUMN data document; 

     CREATE TABLE IF NOT EXISTS document_schema.' || tname || ' (key_id bigint, count bigint, dirty bool, upgraded bool);
     CREATE TRIGGER count_keys AFTER INSERT ON ' || tname::regclass || ' FOR EACH ROW EXECUTE PROCEDURE analyze_document();
     CREATE TRIGGER analyze_schema AFTER INSERT ON ' || tname::regclass || ' FOR EACH STATEMENT EXECUTE PROCEDURE analyze_schema();
     ';
END;
$$ LANGUAGE plpgsql;
