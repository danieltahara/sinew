import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Test {

    public static class Mapper {
        static final String STRING_TYPE = "_str";
        static final String INT_TYPE = "_int";
        static final String FLOAT_TYPE = "_float";
        static final String BOOL_TYPE = "_bool";
        static final String ARRAY_TYPE = "_arr";
        static final String OBJECT_TYPE = "_obj";

        private JSONParser parser = new JSONParser();

        public Map<String, Object> mapFromJSON(String data)
        {
            Map<String, Object> resultSet = new HashMap<String,Object>();
            Map<String, Object> json;
            try {
                json = (Map<String, Object>)parser.parse(data.toString());
            } catch (ParseException e) {
                return resultSet;
            }

            for (Map.Entry<String, Object> entry : json.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                String type = STRING_TYPE;
                if (value == null) {
                    continue;
                } else if (value instanceof Number) {
                    type = INT_TYPE;
                    value = new Integer(((Number)value).intValue());
                } else if (value instanceof Boolean) {
                    type = BOOL_TYPE;
                } else if (value instanceof List) {
                    type = ARRAY_TYPE;
                } else if (value instanceof Map) {
                    type = OBJECT_TYPE;
                }
                resultSet.put(key + type, value);
            }
            return resultSet;
        }
    }

    public static void main(String[] args) throws Exception
    {
        FileReader fileReader = new FileReader(new File(args[0]));
        System.out.println(args[0]);
        BufferedReader br = new BufferedReader(fileReader);

        Mapper mapper = new Mapper();
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            Map<String, Object> nb = mapper.mapFromJSON(line);

            for (Map.Entry<String, Object> entry : nb.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue(); // Guaranteed to be non-null
                System.out.println(key + " : " + value.toString());
                if (value instanceof Map) {
                    for (Map.Entry<String, Object> subentry : ((Map<String, Object>)value).entrySet()) {
                        System.out.println("\t" + subentry.getKey() + " : " + subentry.getValue().toString());
                    }
                } else if (value instanceof List) {
                    for (String str : (List<String>)value) {
                        System.out.print("\t" + str);
                    }
                    System.out.println();
                }
            }
        }
    }
}
