package com.hadapt.catalog;

import java.sql.ResultSet;

public final class Catalog {

    static Catalog catalogInstance = null;

    PostgresWorker _worker;

    public static Catalog getInstance() {
        if (catalogInstance == null) {
            catalogInstance = new Catalog();
        }
        return catalogInstance;
    }

    private Catalog() {
        _worker = PostgresWorkerPool.getDedicatedWorker();
    }

    public ResultSet getDocumentSchema(String relname) {
        return
    }

    public void addToDocumentSchema(String relname) {
        public static final Attribute[] DOC_SCHEMA_ATTRIBUTES = { new Attribute("key_name", "text")};
        throw new Exception("Not implemented");
    }

    public boolean tableExists(String relname) {
        throw new Exception("Not implemented");
    }
}

//    public ArrayList<Attribute> getRelationSchema(String relname) {
//        return _getSchemaInternal(relname, GET_SCHEMA_STATEMENT_TEMPLATE);
//    }
//
//    public ArrayList<Attribute> getDocumentSchema(String relname) {
//        return _getSchemaInternal(relname, GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE);
//    }
//
//    public ArrayList<Attribute> getDocumentSchema(String relname, String extraClause) {
//        return _getSchemaInternal(relname, GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE + " " + extraClause);
//    }
//
//
//    private ArrayList<Attribute> _getSchemaInternal(String relname, String stmtTemplate) {
//        if (!_initialized) {
//            System.err.println("Catalog: need to initialize before use");
//            return new ArrayList<Attribute>();
//        }
//        ArrayList<Attribute> schema = new ArrayList<Attribute>();
//        try {
//            PreparedStatement stmt = _conn.prepareStatement(stmtTemplate);
//            stmt.setString(1, relname);
//            ResultSet rs = stmt.executeQuery();
//            while (rs.next()) {
//                String name = rs.getString("key_name");
//                String type = rs.getString("column_type");
//                schema.add(new Attribute(name, type));
//            }
//            return schema;
//        } catch (SQLException e) {
//            e.printStackTrace();
//            return new ArrayList<Attribute>();
//        }
//
//    }
//
//    public void getExceptions(String relname, String keyname) {
//
//    }
//}
