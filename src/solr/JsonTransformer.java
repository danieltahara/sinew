package com.hadapt.solr;

import org.apache.solr.handler.dataimport.Transformer;
import org.apache.solr.handler.dataimport.Context;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// <doc> tname, _id, document_data <doc>
public class JsonTransformer extends Transformer {
    static final String STRING_TYPE = "_s";
    static final String STRING_ARRAY_TYPE = "_ss";
    static final String INT_TYPE = "_i";
    static final String INT_ARRAY_TYPE = "_is";
    static final String FLOAT_TYPE = "_f";
    static final String FLOAT_ARRAY_TYPE = "_fs";
    static final String BOOL_TYPE = "_b";
    static final String BOOL_ARRAY_TYPE = "_bs";

    static JSONParser parser = new JSONParser(); // Only for use in mapFromJSON

    public Object transformRow(Map<String,Object> row, Context context)
    {
        List<Map<String, String>> fields = context.getAllEntityFields();

        for (Map<String, String> field : fields) {
            // Check if this field has trim="true" specified in the
            // data-config.xml
            String trim = field.get("trim");
            if ("true".equals(trim)) {
                // Apply trim on this field
                String columnName = field.get(DataImporter.COLUMN);
                // Get this fields' value frm the current row
                Object value = row.get(columnName);
                // Transform and put updated values into map
                if (value != null) {
                    row.addAll(mapFromJson(value));
                }
            }
        }
        return row;
    }

    public static Map<String, Object> mapFromJSON(String namespace, Object data)
    {
        Map<String, Object> resultSet = new HashMap<String,Object>(); // FIXME: is this correct?
        Map<String, Object> json;
        String type = STRING_TYPE;
        try {
            json = (Map<String, Object>)parser.parse((String)data); // TODO: Error checking
        } catch (ParseException e) {
            resultSet.put(composeKeyname(namespace, "data", type), data);
            return resultSet;
        }

        for (Map.Entry<String,Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            // TODO: support INT, bool, etc
            if (value == null) {
                value = "";
            } else if (value instanceof Number) {
                type = FLOAT_TYPE;
            } else if (value instanceof Boolean) {
                type = BOOL_TYPE;
            } else { // Convert everything else to string representation
                value = value.toString();
            }
            resultSet.put(composeKeyname(namespace, key, type), value);
        }
        return resultSet;
    }
}
