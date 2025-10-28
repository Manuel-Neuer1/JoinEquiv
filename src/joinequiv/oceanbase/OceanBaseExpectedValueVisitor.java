package joinequiv.oceanbase;

import joinequiv.IgnoreMeException;
import joinequiv.oceanbase.ast.OceanBaseAggregate;
import joinequiv.oceanbase.ast.OceanBaseBinaryComparisonOperation;
import joinequiv.oceanbase.ast.OceanBaseBinaryLogicalOperation;
import joinequiv.oceanbase.ast.OceanBaseCastOperation;
import joinequiv.oceanbase.ast.OceanBaseColumnName;
import joinequiv.oceanbase.ast.OceanBaseColumnReference;
import joinequiv.oceanbase.ast.OceanBaseComputableFunction;
import joinequiv.oceanbase.ast.OceanBaseConstant;
import joinequiv.oceanbase.ast.OceanBaseExists;
import joinequiv.oceanbase.ast.OceanBaseExpression;
import joinequiv.oceanbase.ast.OceanBaseInOperation;
import joinequiv.oceanbase.ast.OceanBaseOrderByTerm;
import joinequiv.oceanbase.ast.OceanBaseSelect;
import joinequiv.oceanbase.ast.OceanBaseStringExpression;
import joinequiv.oceanbase.ast.OceanBaseTableReference;
import joinequiv.oceanbase.ast.OceanBaseText;
import joinequiv.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import joinequiv.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import joinequiv.oceanbase.ast.OceanBaseJoin;

public class OceanBaseExpectedValueVisitor implements OceanBaseVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(OceanBaseExpression expr) {
        OceanBaseToStringVisitor v = new OceanBaseToStringVisitor();
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
    public void visit(OceanBaseExpression expr) {
        nrTabs++;
        try {
            OceanBaseVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(OceanBaseConstant constant) {
        print(constant);
    }

    @Override
    public void visit(OceanBaseColumnReference column) {
        print(column);
    }

    @Override
    public void visit(OceanBaseUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(OceanBaseComputableFunction f) {
        print(f);
        for (OceanBaseExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(OceanBaseBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(OceanBaseSelect select) {
        for (OceanBaseExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(OceanBaseBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(OceanBaseCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseInOperation op) {
        print(op);
        visit(op.getExpr());
        for (OceanBaseExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(OceanBaseOrderByTerm op) {
    }

    @Override
    public void visit(OceanBaseExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseStringExpression op) {
        print(op);
    }

    @Override
    public void visit(OceanBaseTableReference ref) {
    }

    @Override
    public void visit(OceanBaseAggregate aggr) {
    }

    @Override
    public void visit(OceanBaseColumnName aggr) {
    }

    @Override
    public void visit(OceanBaseText func) {
    }

    @Override
    public void visit(OceanBaseUnaryPrefixOperation op) {
        print(op);
        visit(op.getExpr());
    }
    @Override
    public void visit(OceanBaseJoin join) {
        // ExpectedValueVisitor 不应该处理 JOIN 节点，这是一个结构性元素，而不是值的表达式。
        // 如果代码逻辑走到了这里，说明有地方出错了。
        throw new UnsupportedOperationException("Cannot compute the expected value of a JOIN clause.");
    }

}
