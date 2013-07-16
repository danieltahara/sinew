package com.hadapt.handler.rewriter;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.WithItem;

// Limitations: Don't resolve columnindex

public class ColumnRefResolver implements SelectVisitor, FromItemVisitor, ExpressionVisitor,
        ItemsListVisitor, OrderByVisitor, SelectItemVisitor {

    public void resolveColumnRefs(Select select) {
        // TODO: use tablenamefinder from example to get subset of catalogs
        select.getSelectBody().accept(this);
        if (!select.getWithItemsList().isEmpty()) {
            visitWithItems(select.getWithItemsList());
        }
    }

    public void visitWithItems(List withItems) {
        for (Iterator withsIt = withItems.iterator(); withsIt.hasNext();) {
            WithItem with = (WithItem) withsIt.next();
            with.getSelectBody().accept(this);
            if (!with.getWithItemList().isEmpty()) {
                visitWithItems(with.getWithItemList());
            }
        }
    }

    // Bread and butter
    public void visit(Column tableColumn) {
        // FIXME:
        // Look this up in my table
        // get column references
        // if table.hasColumn(cname), let be
        // else if table.hasJSONKey(cname), replace with json_object_field(json_data, 'key_1')
        // else remove it and fill with null -- fix this
        // Just get tables from select?
        // what about qualified column names?
    }

    public void visit(AllColumns allColumns) {
        // For now, don't expand out * to include JSON keys
    }

    public void visit(AllTableColumns allTableColumns) {
        // For now, don't expand out * to include JSON keys
    }

    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);

        if (plainSelect.getJoins() != null) {
            for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
                Join join = (Join) joinsIt.next();
                join.getRightItem().accept(this);
                join.getOnExpression().accept(this);
            }
        }
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }

    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    public void visit(Union union) {
        for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
            PlainSelect plainSelect = (PlainSelect) iter.next();
            visit(plainSelect);
        }
    }

    public void visit(Table tableName) {
    }

    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(this);
    }

    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    public void visit(DoubleValue doubleValue) {
    }

    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    public void visit(Function function) {
    }

    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getItemsList().accept(this);
    }

    public void visit(InverseExpression inverseExpression) {
        inverseExpression.getExpression().accept(this);
    }

    public void visit(IsNullExpression isNullExpression) {
    }

    public void visit(JdbcParameter jdbcParameter) {
    }

    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    public void visit(LongValue longValue) {
    }

    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    public void visit(NullValue nullValue) {
    }

    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    public void visit(StringValue stringValue) {
    }

    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    public void visit(ExpressionList expressionList) {
        for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
        }
    }

    public void visit(DateValue dateValue) {
    }

    public void visit(TimestampValue timestampValue) {
    }

    public void visit(TimeValue timeValue) {
    }

    public void visit(CaseExpression caseExpression) {
        caseExpression.getSwitchExpression().accept(this);
        ExpressionList whenExprs = new ExpressionList(caseExpression.getWhenClauses());
        whenExprs.accept(this);
        caseExpression.getElseExpression().accept(this);
    }

    public void visit(WhenClause whenClause) {
        whenClause.getWhenExpression().accept(this);
        whenClause.getThenExpression().accept(this);
    }

    public void visit(AllComparisonExpression allComparisonExpression) {
        allComparisonExpression.GetSubSelect().getSelectBody().accept(this);
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.GetSubSelect().getSelectBody().accept(this);
    }

    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        subjoin.getJoin().getRightItem().accept(this);
    }

    public void visit(OrderByElement orderBy) {
        orderBy.getExpression().accept(this);
    }
}

