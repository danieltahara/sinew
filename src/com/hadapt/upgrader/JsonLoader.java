package com.hadapt.handler.upgrader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hadapt.catalog.Attribute;
import com.hadapt.catalog.CatalogService;

public class JsonLoader {
    static final String DELIMITER = "|";
    static final String LOAD_TABLE_STATEMENT_TEMPLATE = "COPY ? FROM '?' WITH DELIMITER '|' NULL '' CSV ESCAPE '\\'";

    String _relname;
    Connection _conn;
    String _dataPath;
    String _tmpDataPath;
    JSONParser _parser;
    HashMap<Attribute, Integer> _keyCounts;

    public JsonLoader(String relname, Connection conn, String dataPath) {
        _relname = relname;
        _conn = conn;
        _dataPath = dataPath;
        _parser = new JSONParser();
        _keyCounts = new HashMap<Attribute, Integer>();
    }

    public void loadTable(FileWriter rejects) {
        formatDataInput(rejects);

        // Execute load
        synchronized(_conn) {
            PreparedStatement stmt = null;
            try {
                stmt = _conn.prepareStatement(LOAD_TABLE_STATEMENT_TEMPLATE);
                stmt.setString(1, _relname);
                stmt.setString(2, _tmpDataPath);
                if (stmt.execute()) {
                    System.out.println(stmt.getUpdateCount() + " records inserted");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Update document schema
        CatalogService catalog = CatalogService.getCatalog();
        HashMap<String, Object> baseData = new HashMap<String, Object>();
        baseData.put("table_name", _relname);
        baseData.put("column_name", _relname);
        baseData.put("materialized", new Boolean(false));
        baseData.put("dirty", new Boolean(false));
        for (Map.Entry<Attribute, Integer> entry : _keyCounts.entrySet()) {
            baseData.put("key_name", entry.getKey()._name);
            baseData.put("type", entry.getKey()._type);
            baseData.put("count", entry.getValue());
            catalog.updateDocumentSchema(baseData);
        }
    }

    private void formatDataInput(FileWriter rejects) {
        BufferedReader data = null;
        try {
            data = new BufferedReader(new FileReader(_dataPath));
            File filename = File.createTempFile("ss_", ".tmp");
            filename.deleteOnExit();
            _tmpDataPath = filename.getAbsolutePath();
            FileWriter tmpDataFile = new FileWriter(filename, true);

            String record = null;
            while ((record = data.readLine()) != null) {
                try {
                    JSONObject json = (JSONObject)_parser.parse(record);
                    for (Object key : json.keySet()) {
                        Integer prevCount = 0;
                        Object value = json.get(key);
                        if (value != null) {
                            Attribute attr = new Attribute((String)key, value.getClass().toString());
                            if ((prevCount = _keyCounts.get(attr)) != null) {
                                _keyCounts.put(attr, prevCount + 1);
                            } else {
                                _keyCounts.put(attr, 1);
                            }
                        }
                    }

                    String transformedRecord = "";
                    for (Attribute attr : CatalogService.getCatalog().getRelationSchema(_relname)) {
                        if (json.containsKey(attr._name)) {
                            // TODO: and type is right
                            transformedRecord += json.get(attr._name);
                            transformedRecord += "|";
                            // TODO: else
                            // TODO: add exception to exceptions table
                            json.remove(attr._name);
                        }
                    }
                    transformedRecord += json.toJSONString();

                    tmpDataFile.write(transformedRecord);
                    tmpDataFile.write("\n");
                } catch (ParseException e) {
                    System.err.print(e.getMessage());
                    rejects.write(record);
                    rejects.write("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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

}
