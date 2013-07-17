package com.hadapt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.hadapt.rewriter.PostgresStatement;

// FIXME: Make a worker thread pool
public class PostgresWorker {
    static final String UPDATE_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE =
            "UPDATE information_schema.documents SET count = count + ?, dirty = ? WHERE key_name = '?'" +
                    "AND table_name = '?' AND key_type = '?'";
    static final String INSERT_DOCUMENT_SCHEMA_STATEMENT_TEMPLATE =
            "INSERT INTO information_schema.documents VALUES('?', '?', '?', '?', '?', '?', '?')";
    static final String GET_DOCUMENT_KEYS_STATEMENT_TEMPLATE =
            "SELECT key_name, key_type FROM information_schema.documents WHERE table_name = '?'";
    static final String GET_SCHEMA_STATEMENT_TEMPLATE =
            "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = '?'";
    static final String DELIMITER = "|";
    static final String LOAD_TABLE_STATEMENT_TEMPLATE =
            "COPY ? FROM '?' WITH DELIMITER '" + DELIMITER +
                    "' NULL '' CSV ESCAPE '\\'";

    private Connection _conn;

    public PostgresWorker(String url, Properties props) throws SQLException {
        _conn = DriverManager.getConnection(url, props);
    }

    public void execute(PostgresStatement pgStmt) {
        // Use JDBC driver to perform statement
        try {
            Statement stmt = _conn.createStatement();
            if (stmt.execute(pgStmt.getSql())) {
                print(stmt.getResultSet());
            } else {
                print(stmt.getUpdateCount());
            }
        } catch (SQLException e) {
            System.err.println("SQL exception: " + e.getMessage());
        }
    }

    private void print(ResultSet rs) {
        try {
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int numCols = rsmd.getColumnCount();
                for (int i = 1; i <= numCols; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.err.println("Error printing results: " + e.getMessage());
        }
    }

    private void print(int numUpdated) {
        System.out.println("Updated " + numUpdated + " records");
    }
}
