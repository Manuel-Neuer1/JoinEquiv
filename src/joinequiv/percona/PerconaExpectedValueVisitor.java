package joinequiv.percona;

import joinequiv.IgnoreMeException;
import joinequiv.percona.ast.PerconaCastOperation;
import joinequiv.percona.ast.*;

import java.util.List;

public class PerconaExpectedValueVisitor implements PerconaVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(PerconaExpression expr) {
        PerconaToStringVisitor v = new PerconaToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(PerconaExpression expr) {
        nrTabs++;
        try {
            PerconaVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(PerconaConstant constant) {
        print(constant);
    }

    @Override
    public void visit(PerconaColumnReference column) {
        print(column);
    }

    @Override
    public void visit(PerconaUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(PerconaComputableFunction f) {
        print(f);
        for (PerconaExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(PerconaBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(PerconaSelect select) {
        for (PerconaExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(PerconaBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(PerconaCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(PerconaInOperation op) {
        print(op);
        for (PerconaExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(PerconaBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(PerconaOrderByTerm op) {
    }

    @Override
    public void visit(PerconaExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(PerconaStringExpression op) {
        print(op);
    }

    @Override
    public void visit(PerconaBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(PerconaTableReference ref) {
    }

    @Override
    public void visit(PerconaCollate collate) {
        print(collate);
        visit(collate.getExpectedValue());
    }

    @Override
    public void visit(PerconaJoin join) {
        print(join);
        visit(join.getOnClause());
    }

    @Override
    public void visit(PerconaText text) {
        print(text);
    }

    @Override
    public void visit(PerconaAggregate aggr) {
        // PQS is currently unsupported for aggregates.
        throw new IgnoreMeException();
    }

    @Override
    public void visit(PerconaCaseOperator caseOp) {
        print(caseOp);

        PerconaExpression switchCondition = caseOp.getSwitchCondition();
        if (switchCondition != null) {
            print(switchCondition);
            visit(switchCondition);
        }

        List<PerconaExpression> whenConditions = caseOp.getConditions();
        List<PerconaExpression> thenExpressions = caseOp.getExpressions();

        for (int i = 0; i < whenConditions.size(); i++) {
            print(whenConditions.get(i));
            visit(whenConditions.get(i));
            print(thenExpressions.get(i));
            visit(thenExpressions.get(i));
        }

        PerconaExpression elseExpr = caseOp.getElseExpr();
        if (elseExpr != null) {
            print(elseExpr);
            visit(elseExpr);
        }
    }
}
