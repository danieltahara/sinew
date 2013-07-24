package com.hadapt.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hadapt.PostgresWorker;
import com.hadapt.PostgresWorkerPool;
import com.hadapt.catalog.Attribute;

public class JsonLoader {
    String _relname;
    String _dataPath;
    String _tmpDataPath;
    JSONParser _parser;
    HashMap<Attribute, Integer> _keyCounts;

    public static String pgTypeForValue(Object value) {
        Class valueClass = value.getClass();
        if (Boolean.class.isAssignableFrom(valueClass)) {
            return "boolean";
        } else if (Long.class.isAssignableFrom(valueClass)) {
            return "bigint";
        } else if (Double.class.isAssignableFrom(valueClass)) {
            return "double precision";
        } else {
            return "text";
        }
    }

    public JsonLoader(String relname, String dataPath) {
        _relname = relname;
        _dataPath = dataPath;
        _parser = new JSONParser();
        _keyCounts = new HashMap<Attribute, Integer>();
    }

    public void loadTable(FileWriter rejects) throws SQLException, IllegalStateException {
        formatDataInput(rejects);

        // Execute load
        PostgresWorker worker = PostgresWorkerPool.getInstance().getWorker();
        worker.startTransaction();

        try {
            int count = worker.copyFrom(_relname, _tmpDataPath);
            System.out.println(count + " records inserted");
            // Create document schema table if it doesn't exist
            worker.createTableIfNotExists("document_schema." + _relname,
                    "key_name text, key_type text, materialized boolean," +
                            "dirty boolean, count int, PRIMARY KEY (key_name, key_type)");
            ResultSet rsDocInfo = worker.select("*", "document_schema." + _relname, true);
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
            }
            // For previously non-existent attributes, add them to the document schema
            ArrayList<String> valuesArray = new ArrayList<String>();
            for (Map.Entry<Attribute, Integer> entry : _keyCounts.entrySet()) {
                valuesArray.add("'" + entry.getKey()._name + "', '" + entry.getKey()._type + "', false, true, " +
                        entry.getValue().toString());
            }
            worker.insertValuesBatch("document_schema." + _relname, valuesArray);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        worker.endTransaction();
        PostgresWorkerPool.getInstance().returnWorker(worker);
    }

    private void formatDataInput(FileWriter rejects) {
        BufferedReader data = null;
        try {
            data = new BufferedReader(new FileReader(_dataPath));
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            Date date = new Date();
            File filename = File.createTempFile("load_" + _relname + dateFormat.format(date), ".tmp");
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
            tmpDataFile.flush();
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
