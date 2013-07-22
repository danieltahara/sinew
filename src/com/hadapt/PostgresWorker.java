package com.hadapt;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadapt.catalog.Attribute;

public class PostgresWorker {
    static final String DELIMITER = "|";

    // Query Templates
    static final String SELECT_STMT_TEMPLATE = "SELECT %s FROM %s%s";
    static final String INSERT_VALUES_STMT_TEMPLATE = "INSERT INTO %s VALUES (%s)";
    // DDL Templates
    static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (%s)";
    static final String CREATE_TABLE_IF_NOT_EXISTS_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s (%s)";
    static final String COPY_FROM_TEMPLATE = "COPY %s (json_data) FROM '%s'";
    static final String ADD_COLUMN_TEMPLATE = "ALTER TABLE %s ADD COLUMN %s %s";

    private Connection _conn;
    final Logger _logger = LoggerFactory.getLogger(PostgresWorker.class);

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
        Statement stmt = _conn.createStatement();
        return stmt.executeQuery(String.format(SELECT_STMT_TEMPLATE, targetList, rangeTables, " " + predicates));
    }

    public ResultSet select(String targetList, String rangeTables, boolean updatable) throws SQLException {
        return select(targetList, rangeTables, "", updatable);
    }

    public ResultSet select(String targetList, String rangeTables, String predicates, boolean updatable) throws SQLException {
        Statement stmt = _conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                               updatable ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY);
        return stmt.executeQuery(String.format(SELECT_STMT_TEMPLATE,
                                               targetList,
                                               rangeTables,
                                               " " + predicates));
    }

    public int createTable(String relname, String columns) throws SQLException {
        Statement stmt = _conn.createStatement();
        return stmt.executeUpdate(String.format(CREATE_TABLE_TEMPLATE, relname, columns));
    }

    public int createTableIfNotExists(String relname, String columns) throws SQLException {
        Statement stmt = _conn.createStatement();
        return stmt.executeUpdate(String.format(CREATE_TABLE_IF_NOT_EXISTS_TEMPLATE, relname, columns));
    }

    public int addColumn(String relname, Attribute attr) throws SQLException {
        Statement stmt = _conn.createStatement();
        return stmt.executeUpdate(String.format(ADD_COLUMN_TEMPLATE, relname, attr._name, attr._type));
    }

    public int copyFrom(String relname, String dataPath) throws SQLException {
        _logger.debug(String.format(COPY_FROM_TEMPLATE, relname, dataPath));
        Statement stmt = _conn.createStatement();
        stmt.execute(String.format(COPY_FROM_TEMPLATE, relname, dataPath));
        return 1;
    }

    public int insertValues(String relname, String values) throws SQLException {
        Statement stmt = _conn.createStatement();
        return stmt.executeUpdate(String.format(INSERT_VALUES_STMT_TEMPLATE, relname, values));
    }

    // NOTE: In general, should execute stmt.close()
    public int[] insertValuesBatch(String relname, ArrayList<String> valuesList) throws SQLException {
        _conn.setAutoCommit(false);

        Statement stmt = _conn.createStatement();
        for (String values : valuesList) {
            stmt.addBatch(String.format(INSERT_VALUES_STMT_TEMPLATE, relname, values));
        }

        int[] results = null;
        try {
            results = stmt.executeBatch();
        } catch(SQLException e) {
            while(e.getNextException() != null) {
                e.printStackTrace();
                e = e.getNextException();
            }
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
