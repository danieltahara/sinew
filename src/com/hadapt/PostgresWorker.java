package com.hadapt;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresWorker {
    static final String DELIMITER = "|";

    // Query Templates
    static final String SELECT_STMT_TEMPLATE = "SELECT ? FROM ??";
    static final String INSERT_VALUES_STMT_TEMPLATE = "INSERT INTO ? VALUES (?)";
    // DDL Templates
    static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE ? (?)";
    static final String CREATE_TABLE_IF_NOT_EXISTS_TEMPLATE = "CREATE TABLE IF NOT EXISTS ? (?)";
    static final String COPY_FROM_TEMPLATE =
        "COPY ? FROM '?' WITH DELIMITER " + "'" + DELIMITER + "' " +
        "NULL '' CSV ESCAPE '\\'";

    private Connection _conn;

    public PostgresWorker(Connection conn) {
        _conn = conn;
    }

    public Connection getConnection() {
        return _conn;
    }

    // Internal SQL api/helper functions
    public ResultSet select(String targetList, String rangeTables) throws SQLException {
        return select(targetList, rangeTables, "");
    }

    public ResultSet select(String targetList, String rangeTables, String predicates) throws SQLException {
        PreparedStatement stmt = _conn.prepareStatement(SELECT_STMT_TEMPLATE);
        stmt.setString(1, targetList);
        stmt.setString(2, rangeTables);
        stmt.setString(3, " " + predicates);
        return stmt.executeQuery();
    }

    public int createTable(String relname, String columns) throws SQLException {
        PreparedStatement stmt = _conn.prepareStatement(CREATE_TABLE_TEMPLATE);
        stmt.setString(1, relname);
        stmt.setString(2, columns);
        return stmt.executeUpdate();
    }

    public int createTableIfNotExists(String relname, String columns) throws SQLException {
        PreparedStatement stmt = _conn.prepareStatement(CREATE_TABLE_IF_NOT_EXISTS_TEMPLATE);
        stmt.setString(1, relname);
        stmt.setString(2, columns);
        return stmt.executeUpdate();
    }

    public int copyFrom(String relname, String dataPath) throws SQLException {
        PreparedStatement stmt = _conn.prepareStatement(COPY_FROM_TEMPLATE);
        stmt.setString(1, relname);
        stmt.setString(2, dataPath);
        return stmt.executeUpdate();
    }

    public int insertValues(String relname, String values) throws SQLException {
        PreparedStatement stmt = _conn.prepareStatement(INSERT_VALUES_STMT_TEMPLATE);
        stmt.setString(1, relname);
        stmt.setString(2, values);
        return stmt.executeUpdate();
    }

    // NOTE: In general, should execute stmt.close()
    public int[] insertValuesBatch(String relname, String[] valuesList) throws SQLException {
        _conn.setAutoCommit(false);

        PreparedStatement stmt = _conn.prepareStatement(INSERT_VALUES_STMT_TEMPLATE);
        stmt.setString(1, relname);
        for (String values : valuesList) {
            stmt.setString(2, values);
            stmt.addBatch();
        }

        int[] results = null;
        try {
            results = stmt.executeBatch();
        } catch(BatchUpdateException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) { stmt.close(); }
            _conn.setAutoCommit(true);
        }
        return results;
    }

    public void startTransaction() {
        // TODO: create a transaction object?
        // add to transaction??
    }

    public boolean endTransaction() {
        // _conn.setAutoCommit(false);
        return true;
    }
}
