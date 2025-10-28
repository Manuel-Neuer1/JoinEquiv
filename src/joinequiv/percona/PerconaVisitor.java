package joinequiv.percona;

import joinequiv.percona.ast.PerconaCastOperation;
import joinequiv.percona.ast.*;

public interface PerconaVisitor {

    void visit(PerconaTableReference ref);

    void visit(PerconaConstant constant);

    void visit(PerconaColumnReference column);

    void visit(PerconaUnaryPostfixOperation column);

    void visit(PerconaComputableFunction f);

    void visit(PerconaBinaryLogicalOperation op);

    void visit(PerconaSelect select);

    void visit(PerconaBinaryComparisonOperation op);

    void visit(PerconaCastOperation op);

    void visit(PerconaInOperation op);

    void visit(PerconaBinaryOperation op);

    void visit(PerconaOrderByTerm op);

    void visit(PerconaExists op);

    void visit(PerconaStringExpression op);

    void visit(PerconaBetweenOperation op);

    void visit(PerconaCollate collate);

    void visit(PerconaJoin join);

    void visit(PerconaText text);

    void visit(PerconaAggregate aggregate);

    void visit(PerconaCaseOperator caseOp);

    default void visit(PerconaExpression expr) {
        if (expr instanceof PerconaConstant) {
            visit((PerconaConstant) expr);
        } else if (expr instanceof PerconaColumnReference) {
            visit((PerconaColumnReference) expr);
        } else if (expr instanceof PerconaUnaryPostfixOperation) {
            visit((PerconaUnaryPostfixOperation) expr);
        } else if (expr instanceof PerconaComputableFunction) {
            visit((PerconaComputableFunction) expr);
        } else if (expr instanceof PerconaBinaryLogicalOperation) {
            visit((PerconaBinaryLogicalOperation) expr);
        } else if (expr instanceof PerconaSelect) {
            visit((PerconaSelect) expr);
        } else if (expr instanceof PerconaBinaryComparisonOperation) {
            visit((PerconaBinaryComparisonOperation) expr);
        } else if (expr instanceof PerconaCastOperation) {
            visit((PerconaCastOperation) expr);
        } else if (expr instanceof PerconaInOperation) {
            visit((PerconaInOperation) expr);
        } else if (expr instanceof PerconaBinaryOperation) {
            visit((PerconaBinaryOperation) expr);
        } else if (expr instanceof PerconaOrderByTerm) {
            visit((PerconaOrderByTerm) expr);
        } else if (expr instanceof PerconaExists) {
            visit((PerconaExists) expr);
        } else if (expr instanceof PerconaJoin) {
            visit((PerconaJoin) expr);
        } else if (expr instanceof PerconaStringExpression) {
            visit((PerconaStringExpression) expr);
        } else if (expr instanceof PerconaBetweenOperation) {
            visit((PerconaBetweenOperation) expr);
        } else if (expr instanceof PerconaTableReference) {
            visit((PerconaTableReference) expr);
        } else if (expr instanceof PerconaCollate) {
            visit((PerconaCollate) expr);
        } else if (expr instanceof PerconaText) {
            visit((PerconaText) expr);
        } else if (expr instanceof PerconaAggregate) {
            visit((PerconaAggregate) expr);
        } else if (expr instanceof PerconaCaseOperator) {
            visit((PerconaCaseOperator) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(PerconaExpression expr) {
        PerconaToStringVisitor visitor = new PerconaToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(PerconaExpression expr) {
        PerconaExpectedValueVisitor visitor = new PerconaExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
