package com.hadapt.rewriter;


import net.sf.jsqlparser.statement.Statement;

// Wrapper for a JSQLParser statement. Stores raw string in event that statement is valid PSQL but not valid SQL
public class PostgresStatement {
    private String _rawStatement;
    private Statement _statement;

    public PostgresStatement(String raw) {
        _rawStatement = raw;
        _statement = null;
    }

    public PostgresStatement(String raw, Statement stmt) {
        _rawStatement = raw;
        _statement = stmt;
    }

    public String getSql() {
        if (_statement != null) {
            return _statement.toString();
        } else {
            return _rawStatement;
        }
    }
}
