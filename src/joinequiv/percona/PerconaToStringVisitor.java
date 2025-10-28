package joinequiv.percona;

import joinequiv.Randomly;
import joinequiv.common.visitor.ToStringVisitor;
import joinequiv.percona.ast.PerconaAggregate.PerconaAggregateFunction;
import joinequiv.percona.ast.PerconaCastOperation;
import joinequiv.percona.ast.PerconaOrderByTerm.PerconaOrder;
import joinequiv.percona.ast.*;

import java.util.List;
import java.util.stream.Collectors;

public class PerconaToStringVisitor extends ToStringVisitor<PerconaExpression> implements PerconaVisitor {

    int ref;

    @Override
    public void visitSpecific(PerconaExpression expr) {
        PerconaVisitor.super.visit(expr);
    }

    @Override
    public void visit(PerconaSelect s) {
        sb.append("SELECT ");
        if (s.getHint() != null) {
            sb.append("/*+ ");
            visit(s.getHint());
            sb.append("*/ ");
        }
        switch (s.getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
//        case DISTINCTROW:
//            sb.append("DISTINCTROW ");
//            break;
        default:
            throw new AssertionError();
        }
        sb.append(s.getModifiers().stream().collect(Collectors.joining(" ")));
        if (!s.getModifiers().isEmpty()) {
            sb.append(" ");
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                // Percona does not allow duplicate column names
                sb.append(" AS ");
                sb.append("ref");
                sb.append(ref++);
            }
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        for (PerconaExpression j : s.getJoinList()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            PerconaExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && !s.getGroupByExpressions().isEmpty()) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<PerconaExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            List<PerconaExpression> orderBys = s.getOrderByClauses();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getOrderByClauses().get(i));
            }
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(PerconaConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(PerconaColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(PerconaUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
        case IS_FALSE:
            sb.append("FALSE");
            break;
        case IS_NULL:
            if (Randomly.getBoolean()) {
                sb.append("UNKNOWN");
            } else {
                sb.append("NULL");
            }
            break;
        case IS_TRUE:
            sb.append("TRUE");
            break;
        default:
            throw new AssertionError(op);
        }
    }

    @Override
    public void visit(PerconaComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(PerconaBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(PerconaBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(PerconaCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ");
        sb.append(op.getType());
        sb.append(")");
    }

    @Override
    public void visit(PerconaInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(PerconaBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(PerconaOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == PerconaOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(PerconaExists op) {
        sb.append(" EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(PerconaStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(PerconaBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(PerconaTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(PerconaCollate collate) {
        sb.append("(");
        visit(collate.getExpression());
        sb.append(" ");
        sb.append(collate.getOperatorRepresentation());
        sb.append(")");
    }

    @Override
    public void visit(PerconaJoin join) {
        sb.append(" ");
        switch (join.getType()) {
        case NATURAL:
            sb.append("NATURAL ");
            break;
        case INNER:
            sb.append("INNER ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_");
            break;
        case LEFT:
            sb.append("LEFT ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            break;
        case CROSS:
            sb.append("CROSS ");
            break;
        default:
            throw new AssertionError(join.getType());
        }
        sb.append("JOIN ");
        sb.append(join.getTable().getName());
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

    @Override
    public void visit(PerconaText text) {
        sb.append(text.getText());
    }

    @Override
    public void visit(PerconaAggregate aggr) {
        PerconaAggregateFunction func = aggr.getFunc();
        String option = func.getOption();
        List<PerconaExpression> exprs = aggr.getExprs();

        sb.append(func.getName());
        sb.append("(");
        if (option != null) {
            sb.append(option);
            sb.append(" ");
        }
        for (int i = 0; i < exprs.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(exprs.get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(PerconaCaseOperator caseOp) {
        sb.append("(CASE ");

        if (caseOp.getSwitchCondition() != null) {
            visit(caseOp.getSwitchCondition());
            sb.append(" ");
        }

        for (int i = 0; i < caseOp.getConditions().size(); i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("WHEN ");
            visit(caseOp.getConditions().get(i));
            sb.append(" THEN ");
            visit(caseOp.getExpressions().get(i));
        }

        if (caseOp.getElseExpr() != null) {
            sb.append(" ELSE ");
            visit(caseOp.getElseExpr());
        }
        sb.append(" END)");
    }
}
