package com.hadapt.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.hadapt.handler.rewriter.Parser;
import com.hadapt.handler.rewriter.PostgresStatement;

public class PostgresWorker {
    private Connection _conn;
    private Parser _parser;

    public PostgresWorker(String url, Properties props) throws SQLException {
        _conn = DriverManager.getConnection(url, props);
        _parser = new Parser();
    }

    public void execute(String statement) {
        PostgresStatement pgStmt = _parser.parse(statement);

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

    public void print(ResultSet rs) {
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

    public void print(int numUpdated) {
        System.out.println("Updated " + numUpdated + " records");
    }
}
