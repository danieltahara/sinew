package com.hadapt.handler.upgrader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hadapt.handler.catalog.CatalogService;

public class JsonLoader {
    static final String DELIMITER = "|";
    static final String GET_SCHEMA_STATEMENT_TEMPLATE = "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = '?'";
    static final String LOAD_TABLE_STATEMENT_TEMPLATE = "COPY ? FROM '?' WITH DELIMITER '|' NULL '' CSV ESCAPE '\\'";

    String _relname;
    Connection _conn;
    String _dataPath;
    String _outPath;
    JSONParser _parser;
    ResultSet _schema;
    int _jsonDataIndex = 0;
    int _numRecords = 0;
    HashMap<String, Integer> _keyCounts;

    public JsonLoader(String relname, Connection conn, String dataPath, String outPath) {
        _relname = relname;
        _conn = conn;
        _dataPath = dataPath;
        _outPath = outPath;
        _parser = new JSONParser();
        _keyCounts = new HashMap<String, Integer>();
    }

    public void loadTable(FileWriter rejects) {
        getSchema();
        validateAndCountKeys(rejects);

        // Execute load
        PreparedStatement stmt = null;
        try {
            stmt = _conn.prepareStatement(LOAD_TABLE_STATEMENT_TEMPLATE);
            stmt.setString(1, _relname);
            stmt.setString(2, _dataPath);
            if (stmt.execute()) {
                _numRecords = stmt.getUpdateCount();
            }
        } catch (SQLException e) {
            System.err.print(e.getMessage());
        }

        CatalogService catalog = CatalogService.getCatalog();
        HashMap<String, Object> baseData = new HashMap<String, Object>();
        baseData.put("table_name", _relname);
        baseData.put("column_name", _relname); // FIXME
        baseData.put("materialized", new Boolean(false));
        baseData.put("dirty", new Boolean(false));
        for (Map.Entry<String, Integer> entry : _keyCounts) {
            baseData.put("key_name", entry.getKey());
            baseData.put("type", entry.getValue());
            baseData.put("count", entry.getValue()) // FIXME;
            catalog.updateDocumentSchema(baseData);
        }
    }

    private void validateAndCountKeys(FileWriter rejects) {
        BufferedReader data = null;
        try {
            data = new BufferedReader(new FileReader(_dataPath));

            String record;
            while ((record = data.readLine()) != null) {
                String[] attributes = record.split(DELIMITER);
                try {
                    JSONObject json = (JSONObject)_parser.parse(attributes[_jsonDataIndex]);
                    for (Object key : json.keySet()) {
                        // TODO: mapr - output <key, 1>
                        Integer prevCount = 0;
                        if ((prevCount = _keyCounts.get((String) key)) != null) {
                            _keyCounts.put((String) key, prevCount + 1);
                        } else {
                            _keyCounts.put((String) key, 1);
                        }
                        // TODO: maybe include record count in this hash?
                    }
                } catch (ParseException e) {
                    System.err.print(e.getMessage());
                    rejects.write(record);
                }

            }
        } catch (IOException e) {
            System.err.print(e.getMessage());
        } finally { // TODO: clean me
            try {
                if (data != null) {
                    data.close();
                }
            } catch (IOException e) {
                System.err.print(e.getMessage());
            }
        }

    }

    public void getSchema() { // FIXME: Make this through catalog
        try {
            PreparedStatement stmt = _conn.prepareStatement(GET_SCHEMA_STATEMENT_TEMPLATE);
            stmt.setString(1, _relname);
            _schema = stmt.executeQuery();
            int i = 0;
            while (_schema.next()) {
                String type = _schema.getString("column_type");
                if (type.matches("json")) {
                    _jsonDataIndex = i;
                }
                ++i;
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return;
        }
    }
}
