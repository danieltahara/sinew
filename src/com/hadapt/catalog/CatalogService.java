package com.hadapt.catalog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public final class CatalogService {
    static final String CREATE_DOCUMENT_SCHEMA_STATEMEMT =
        "CREATE TABLE IF NOT EXISTS information_schema.documents(" +
        "key_name text, table_name text, column_name text," +
        "key_type text, materialized boolean, clean boolean, count int," +
        "UNIQUE (key_name, table_name, key_type))";
    // static final String CREATE_DOCUMENT_EXCEPTIONS_TABLE_STATEMENT =
    //         "CREATE TABLE IF NOT EXISTS information_schema.documents(" +
    //         "    key_name text, table_name text, column_name text," +
    //         "    type text, materialized boolean, clean boolean, count int," +
    //         "    UNIQUE (key_name, table_name));"
    static final String UPDATE_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE =
        "UPDATE information_schema.documents SET count = count + ?, dirty = ? WHERE key_name = '?'" +
        "AND table_name = '?' AND key_type = '?'";
    static final String INSERT_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE =
        "INSERT INTO information_schema.documents VALUES('?', '?', '?', '?', '?', '?', '?')";
    static final String GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE =
        "SELECT key_name, key_type FROM information_schema.documents WHERE table_name = '?'";
    static final String GET_SCHEMA_STATEMENT_TEMPLATE =
        "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = '?'";

    static CatalogService catalogInstance = null;

    Connection _conn = null;
    boolean _initialized = false;

    public static CatalogService getCatalog() {
        if (catalogInstance == null) {
            catalogInstance = new CatalogService();
        }
        return catalogInstance;
    }

    private CatalogService() {
    }

    public void initialize(String url, Properties props) throws SQLException {
        if (_conn != null) {
            _conn.close();
        }
        _conn = DriverManager.getConnection(url, props);
        Statement stmt = _conn.createStatement();
        stmt.execute(CREATE_DOCUMENT_SCHEMA_STATEMEMT);
        _initialized = true;
    }

    public void close() {
        try {
            _conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateDocumentSchema(HashMap<String, Object> recordData) {
        if (!_initialized) {
            System.err.println("CatalogService: need to initialize before use");
            return;
        }
        try {
            PreparedStatement stmt = _conn.prepareStatement(UPDATE_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE);
            stmt.setInt(1, (Integer) recordData.get("count"));
            stmt.setBoolean(2, (Boolean)recordData.get("dirty"));
            stmt.setString(3, (String)recordData.get("key_name"));
            stmt.setString(4, (String)recordData.get("table_name"));
            stmt.setString(5, (String)recordData.get("key_type"));
            if (!stmt.execute()) { // Returned an update count instead of a ResultSet (i.e. correct result)
                if (stmt.getUpdateCount() == 0) {
                    stmt = _conn.prepareStatement(INSERT_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE);
                    stmt.setString(1, (String)recordData.get("key_name"));
                    stmt.setString(2, (String)recordData.get("table_name"));
                    stmt.setString(3, (String) recordData.get("column_name"));
                    stmt.setString(4, (String)recordData.get("key_type"));
                    stmt.setBoolean(5, (Boolean) recordData.get("materialized"));
                    stmt.setBoolean(6, (Boolean)recordData.get("dirty"));
                    stmt.setInt(7, (Integer) recordData.get("count"));
                    stmt.execute();
                } else if (stmt.getUpdateCount() != 1) {
                    System.err.println("CatalogService: Updated more than 1 entry");
                }
            }
            else
            {
                System.err.println("CatalogService: Updated returned a resultset");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Attribute> getRelationSchema(String relname) {
        return _getSchemaInternal(relname, GET_SCHEMA_STATEMENT_TEMPLATE);
    }

    public ArrayList<Attribute> getDocumentSchema(String relname) {
        return _getSchemaInternal(relname, GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE);
    }

    public ArrayList<Attribute> getDocumentSchema(String relname, String extraClause) {
        return _getSchemaInternal(relname, GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE + " " + extraClause);
    }


    private ArrayList<Attribute> _getSchemaInternal(String relname, String stmtTemplate) {
        if (!_initialized) {
            System.err.println("CatalogService: need to initialize before use");
            return new ArrayList<Attribute>();
        }
        ArrayList<Attribute> schema = new ArrayList<Attribute>();
        try {
            PreparedStatement stmt = _conn.prepareStatement(stmtTemplate);
            stmt.setString(1, relname);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("key_name");
                String type = rs.getString("column_type");
                schema.add(new Attribute(name, type));
            }
            return schema;
        } catch (SQLException e) {
            e.printStackTrace();
            return new ArrayList<Attribute>();
        }

    }

    public void getExceptions(String relname, String keyname) {

    }
}
