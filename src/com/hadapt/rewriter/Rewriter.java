package com.hadapt.rewriter;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;

public class Rewriter {
    CCJSqlParserManager _jsqlParser;

    public Rewriter() {
        _jsqlParser = new CCJSqlParserManager();

    }

    public String rewrite(String query) {
        Statement stmt = null;
        try {
            stmt = _jsqlParser.parse(new StringReader(query));
            if (stmt instanceof Select) {
                return resolveColumnReferences((Select)stmt).toString();
            } else {
                return stmt.toString();
            }
        } catch (JSQLParserException e) {
            // _looger.debug(parseerror; seeing if pg can handle it);
            return query;
        }
    }

    public Statement resolveColumnReferences(Select select) {
        String[] tnames = extractTableNames(select);

        select.setSelectBody(resolve(select.getSelectBody(), tnames));
        if (!select.getWithItemsList().isEmpty()) {
            select.setWithItemsList(resolveWithItems(select.getWithItemsList(), tnames));
        }
        return select;
    }

    public List resolveWithItems(List withItems) {
        for (Iterator withsIt = withItems.iterator(); withsIt.hasNext();) {
            WithItem with = (WithItem) withsIt.next();
            with.setSelectBody(resolve(with.getSelectBody()));
            if (!with.getWithItemList().isEmpty()) {
                with.setWithItemList(resolveWithItems(with.getWithItemList()));
            }
        }
        return withItems;
    }

    // // Bread and butter
    // public void visit(Column tableColumn) {
    //     // NOTE: Column has table as a separate attribute, so I don't need to worry about it
    //     try {
    //         for (String tname : _tnames) {
    //             PostgresWorker worker = PostgresWorkerPool.getInstance().getWorker();
    //             ResultSet rsDocSchema = worker.select("*", "document_schema." + tname);
    //             while (rsDocSchema.next()) {

    //                 // FIXME: taking a risk with type mismatches

    //             }
    //         }
    //     } catch (SQLException e) {
    //         System.err.println("ColumnRefResolver: DB error - Could not validate column references");
    //         e.printStackTrace();
    //     }
    //     return;
    // }

    // public void visit(AllColumns allColumns) {
    //     // For now, don't expand out * to include JSON keys
    // }

    // public void visit(AllTableColumns allTableColumns) {
    //     // For now, don't expand out * to include JSON keys
    // }

    // public void visit(PlainSelect plainSelect) {
    //     plainSelect.getFromItem().accept(this);

    //     if (plainSelect.getJoins() != null) {
    //         for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
    //             Join join = (Join) joinsIt.next();
    //             join.getRightItem().accept(this);
    //             join.getOnExpression().accept(this);
    //         }
    //     }
    //     if (plainSelect.getWhere() != null) {
    //         plainSelect.getWhere().accept(this);
    //     }

    // }

    // public void visit(SelectExpressionItem selectExpressionItem) {
    //     selectExpressionItem.getExpression().accept(this);
    // }

    // public void visit(Union union) {
    //     for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
    //         PlainSelect plainSelect = (PlainSelect) iter.next();
    //         visit(plainSelect);
    //     }
    // }

    // public void visit(Table tableName) {
    // }

    // public void visit(SubSelect subSelect) {
    //     subSelect.getSelectBody().accept(this);
    // }

    // public void visit(Addition addition) {
    //     visitBinaryExpression(addition);
    // }

    // public void visit(AndExpression andExpression) {
    //     visitBinaryExpression(andExpression);
    // }

    // public void visit(Between between) {
    //     between.getLeftExpression().accept(this);
    //     between.getBetweenExpressionStart().accept(this);
    //     between.getBetweenExpressionEnd().accept(this);
    // }

    // public void visit(Division division) {
    //     visitBinaryExpression(division);
    // }

    // public void visit(DoubleValue doubleValue) {
    // }

    // public void visit(EqualsTo equalsTo) {
    //     visitBinaryExpression(equalsTo);
    // }

    // public void visit(Function function) {
    // }

    // public void visit(GreaterThan greaterThan) {
    //     visitBinaryExpression(greaterThan);
    // }

    // public void visit(GreaterThanEquals greaterThanEquals) {
    //     visitBinaryExpression(greaterThanEquals);
    // }

    // public void visit(InExpression inExpression) {
    //     inExpression.getLeftExpression().accept(this);
    //     inExpression.getItemsList().accept(this);
    // }

    // public void visit(InverseExpression inverseExpression) {
    //     inverseExpression.getExpression().accept(this);
    // }

    // public void visit(IsNullExpression isNullExpression) {
    // }

    // public void visit(JdbcParameter jdbcParameter) {
    // }

    // public void visit(LikeExpression likeExpression) {
    //     visitBinaryExpression(likeExpression);
    // }

    // public void visit(ExistsExpression existsExpression) {
    //     existsExpression.getRightExpression().accept(this);
    // }

    // public void visit(LongValue longValue) {
    // }

    // public void visit(MinorThan minorThan) {
    //     visitBinaryExpression(minorThan);
    // }

    // public void visit(MinorThanEquals minorThanEquals) {
    //     visitBinaryExpression(minorThanEquals);
    // }

    // public void visit(Multiplication multiplication) {
    //     visitBinaryExpression(multiplication);
    // }

    // public void visit(NotEqualsTo notEqualsTo) {
    //     visitBinaryExpression(notEqualsTo);
    // }

    // public void visit(NullValue nullValue) {
    // }

    // public void visit(OrExpression orExpression) {
    //     visitBinaryExpression(orExpression);
    // }

    // public void visit(Parenthesis parenthesis) {
    //     parenthesis.getExpression().accept(this);
    // }

    // public void visit(StringValue stringValue) {
    // }

    // public void visit(Subtraction subtraction) {
    //     visitBinaryExpression(subtraction);
    // }

    // public void visitBinaryExpression(BinaryExpression binaryExpression) {
    //     binaryExpression.getLeftExpression().accept(this);
    //     binaryExpression.getRightExpression().accept(this);
    // }

    // public void visit(ExpressionList expressionList) {
    //     for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
    //         Expression expression = (Expression) iter.next();
    //         expression.accept(this);
    //     }
    // }

    // public void visit(DateValue dateValue) {
    // }

    // public void visit(TimestampValue timestampValue) {
    // }

    // public void visit(TimeValue timeValue) {
    // }

    // public void visit(CaseExpression caseExpression) {
    //     caseExpression.getSwitchExpression().accept(this);
    //     ExpressionList whenExprs = new ExpressionList(caseExpression.getWhenClauses());
    //     whenExprs.accept(this);
    //     caseExpression.getElseExpression().accept(this);
    // }

    // public void visit(WhenClause whenClause) {
    //     whenClause.getWhenExpression().accept(this);
    //     whenClause.getThenExpression().accept(this);
    // }

    // public void visit(AllComparisonExpression allComparisonExpression) {
    //     allComparisonExpression.GetSubSelect().getSelectBody().accept(this);
    // }

    // public void visit(AnyComparisonExpression anyComparisonExpression) {
    //     anyComparisonExpression.GetSubSelect().getSelectBody().accept(this);
    // }

    // public void visit(Concat concat) {
    //     visitBinaryExpression(concat);
    // }

    // public void visit(Matches matches) {
    //     visitBinaryExpression(matches);
    // }

    // public void visit(BitwiseAnd bitwiseAnd) {
    //     visitBinaryExpression(bitwiseAnd);
    // }

    // public void visit(BitwiseOr bitwiseOr) {
    //     visitBinaryExpression(bitwiseOr);
    // }

    // public void visit(BitwiseXor bitwiseXor) {
    //     visitBinaryExpression(bitwiseXor);
    // }

    // public void visit(SubJoin subjoin) {
    //     subjoin.getLeft().accept(this);
    //     subjoin.getJoin().getRightItem().accept(this);
    // }

    // public void visit(OrderByElement orderBy) {
    //     orderBy.getExpression().accept(this);
    // }
}
