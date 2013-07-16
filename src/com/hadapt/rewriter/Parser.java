package com.hadapt.handler.rewriter;

import java.io.StringReader;

import com.hadapt.handler.catalog.CatalogService;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class Parser {
    CCJSqlParserManager _jsqlParser;
    CatalogService _catalog;
    ColumnRefResolver _resolver;

    public Parser(CatalogService catalog) {
        _catalog = catalog;
        _jsqlParser = new CCJSqlParserManager();
        _resolver = new ColumnRefResolver(_catalog);
    }

    public PostgresStatement parse(String command) {
        Statement stmt;
        try {
            stmt = _jsqlParser.parse(new StringReader(command));
        } catch (JSQLParserException e) {
            return new PostgresStatement(command); // Postgres might understand the query, so return as is
        }

        // Manipulate the tree based on the catalog

        // check where clause, etc for join conditions - pushdown to solr
        if (stmt instanceof Select) {
            _resolver.resolveColumnRefs(stmt);
            // _optimizer.optimize(stmt);
        } else if (stmt instanceof Insert) {
            // TODO:
            // Leave for now
            // iterate over values; if ctype = json, parse it
            // see what columns i have
            // if all - rewrite as those keys
            // if some and i'm under limit - rewrite some as keys, leave json_slop;
                // add to table_name.json_column.keys
            // if none - all slop
        } else if (stmt instanceof CreateTable) {
        } else if (stmt instanceof Drop) {
            // if I have a json column, add to catalog
            // get column names/types
        } else if (stmt instanceof Update) {
            // TODO: two cases, new key and updating old
            // Leave it be for now, but definitely want to deal with update, delete, etc
        } else {
            _resolver.resolveColumnRefs(stmt);
            // _optimizer.optimize(stmt);
        }

        return new PostgresStatement(command, stmt);
    }
}
