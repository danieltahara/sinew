package com.hadapt.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hadapt.PostgresWorker;
import com.hadapt.catalog.Attribute;

public class JsonLoader {
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
        PostgresWorker worker = PostgresWorkerPool.getInstance.getWorker();
        worker.startTransaction();

        // NOTE: For now, assume column called "json_data"
        try {
            int count = worker.copyFrom(_relname, _tmpDataPath);
            System.out.println(count + " records inserted");
            ResultSet rsDocInfo = worker.select("*",
                    "information_schema.documents",
                    "WHERE table_name = " + _relname " AND count >= " + minCount);
            while (rsDocInfo.next()) {
                Attribute attr = new Attribute(rsDocInfo.getString("key_name"),
                    rsDocInfo.getString("key_type"));
                if (_keyCounts.containsKey(attr)) {
                    int prevCount = rsDocInfo.getInt("count");
                    rsDocInfo.updateInt("count", prevCount + _keyCounts.get(attr));
                    rsDocInfo.updateBoolean("dirty", true);
                    rsDocInfo.updateRow();
                    _keyCounts.remove(attr);
                }
                // For previously non-existent attributes, add them to the document schema
                for (Map.Entry<Attribute, Integer> entry : _keyCounts.entrySet()) {
                    String[] values = { entry.getKey()._name,_relname,
                        "json_data", entry.getKey()._type, "false", "true",
                        entry.getValue().toString() };
                    worker.insertValues("information_schema.documents", values);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        worker.endTransaction();
        PostgresWorkerPool.getInstance.returnWorker(worker);
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
                            Attribute attr = new Attribute((String)key,
                                pgTypeForValue(value));
                            if ((prevCount = _keyCounts.get(attr)) != null) {
                                _keyCounts.put(attr, prevCount + 1);
                            } else {
                                _keyCounts.put(attr, 1);
                            }
                        }
                    }

                    tmpDataFile.write(record);
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
                e.printStackTrace();
            }
        }

    }

}
