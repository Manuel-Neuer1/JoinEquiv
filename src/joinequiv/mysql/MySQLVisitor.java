package joinequiv.mysql;

import joinequiv.mysql.ast.MySQLAggregate;
import joinequiv.mysql.ast.MySQLBetweenOperation;
import joinequiv.mysql.ast.MySQLBinaryComparisonOperation;
import joinequiv.mysql.ast.MySQLBinaryLogicalOperation;
import joinequiv.mysql.ast.MySQLBinaryOperation;
import joinequiv.mysql.ast.MySQLCaseOperator;
import joinequiv.mysql.ast.MySQLCastOperation;
import joinequiv.mysql.ast.MySQLCollate;
import joinequiv.mysql.ast.MySQLColumnReference;
import joinequiv.mysql.ast.MySQLComputableFunction;
import joinequiv.mysql.ast.MySQLConstant;
import joinequiv.mysql.ast.MySQLExists;
import joinequiv.mysql.ast.MySQLExpression;
import joinequiv.mysql.ast.MySQLInOperation;
import joinequiv.mysql.ast.MySQLJoin;
import joinequiv.mysql.ast.MySQLOrderByTerm;
import joinequiv.mysql.ast.MySQLSelect;
import joinequiv.mysql.ast.MySQLStringExpression;
import joinequiv.mysql.ast.MySQLTableReference;
import joinequiv.mysql.ast.MySQLText;
import joinequiv.mysql.ast.MySQLUnaryPostfixOperation;

public interface MySQLVisitor {

    void visit(MySQLTableReference ref);

    void visit(MySQLConstant constant);

    void visit(MySQLColumnReference column);

    void visit(MySQLUnaryPostfixOperation column);

    void visit(MySQLComputableFunction f);

    void visit(MySQLBinaryLogicalOperation op);

    void visit(MySQLSelect select);

    void visit(MySQLBinaryComparisonOperation op);

    void visit(MySQLCastOperation op);

    void visit(MySQLInOperation op);

    void visit(MySQLBinaryOperation op);

    void visit(MySQLOrderByTerm op);

    void visit(MySQLExists op);

    void visit(MySQLStringExpression op);

    void visit(MySQLBetweenOperation op);

    void visit(MySQLCollate collate);

    void visit(MySQLJoin join);

    void visit(MySQLText text);

    void visit(MySQLAggregate aggregate);

    void visit(MySQLCaseOperator caseOp);

    default void visit(MySQLExpression expr) {
        if (expr instanceof MySQLConstant) {
            visit((MySQLConstant) expr);
        } else if (expr instanceof MySQLColumnReference) {
            visit((MySQLColumnReference) expr);
        } else if (expr instanceof MySQLUnaryPostfixOperation) {
            visit((MySQLUnaryPostfixOperation) expr);
        } else if (expr instanceof MySQLComputableFunction) {
            visit((MySQLComputableFunction) expr);
        } else if (expr instanceof MySQLBinaryLogicalOperation) {
            visit((MySQLBinaryLogicalOperation) expr);
        } else if (expr instanceof MySQLSelect) {
            visit((MySQLSelect) expr);
        } else if (expr instanceof MySQLBinaryComparisonOperation) {
            visit((MySQLBinaryComparisonOperation) expr);
        } else if (expr instanceof MySQLCastOperation) {
            visit((MySQLCastOperation) expr);
        } else if (expr instanceof MySQLInOperation) {
            visit((MySQLInOperation) expr);
        } else if (expr instanceof MySQLBinaryOperation) {
            visit((MySQLBinaryOperation) expr);
        } else if (expr instanceof MySQLOrderByTerm) {
            visit((MySQLOrderByTerm) expr);
        } else if (expr instanceof MySQLExists) {
            visit((MySQLExists) expr);
        } else if (expr instanceof MySQLJoin) {
            visit((MySQLJoin) expr);
        } else if (expr instanceof MySQLStringExpression) {
            visit((MySQLStringExpression) expr);
        } else if (expr instanceof MySQLBetweenOperation) {
            visit((MySQLBetweenOperation) expr);
        } else if (expr instanceof MySQLTableReference) {
            visit((MySQLTableReference) expr);
        } else if (expr instanceof MySQLCollate) {
            visit((MySQLCollate) expr);
        } else if (expr instanceof MySQLText) {
            visit((MySQLText) expr);
        } else if (expr instanceof MySQLAggregate) {
            visit((MySQLAggregate) expr);
        } else if (expr instanceof MySQLCaseOperator) {
            visit((MySQLCaseOperator) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(MySQLExpression expr) {
        MySQLToStringVisitor visitor = new MySQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MySQLExpression expr) {
        MySQLExpectedValueVisitor visitor = new MySQLExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
