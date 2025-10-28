package joinequiv.oceanbase;

import java.util.List;
import java.util.stream.Collectors;

import joinequiv.Randomly;
import joinequiv.common.visitor.ToStringVisitor;
//import joinequiv.mysql.ast.MySQLJoin;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseDataType;
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
import joinequiv.oceanbase.ast.OceanBaseOrderByTerm.OceanBaseOrder;
import joinequiv.oceanbase.ast.OceanBaseSelect;
import joinequiv.oceanbase.ast.OceanBaseStringExpression;
import joinequiv.oceanbase.ast.OceanBaseTableReference;
import joinequiv.oceanbase.ast.OceanBaseText;
import joinequiv.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import joinequiv.oceanbase.ast.OceanBaseUnaryPrefixOperation;
import joinequiv.oceanbase.ast.OceanBaseJoin;

public class OceanBaseToStringVisitor extends ToStringVisitor<OceanBaseExpression> implements OceanBaseVisitor {

    int ref;
    private final Randomly r = new Randomly();

    @Override
    public void visitSpecific(OceanBaseExpression expr) {
        OceanBaseVisitor.super.visit(expr);
    }

    @Override
    public void visit(OceanBaseSelect s) {
        sb.append("SELECT ");
        if (s.getHint() != null) {
            sb.append("/*+ ");
            visit(s.getHint(), 0);
            sb.append(" */ ");
        }

        switch (s.getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
            //TODO 这里就注解掉了
//        case ALL:
//            sb.append(Randomly.fromOptions("ALL ", ""));
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
            }
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        for (OceanBaseExpression j : s.getJoinList()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            OceanBaseExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && !s.getGroupByExpressions().isEmpty()) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<OceanBaseExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());
        }
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            List<OceanBaseExpression> orderBys = s.getOrderByClauses();
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
    public void visit(OceanBaseConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(OceanBaseColumnReference column) {
        if (column.getColumn().getType() == OceanBaseDataType.FLOAT || column.getColumn().isZeroFill()) {
            sb.append("concat(");
        }
        sb.append(column.getColumn().getFullQualifiedName());
        if (column.getColumn().getType() == OceanBaseDataType.FLOAT || column.getColumn().isZeroFill()) {
            sb.append(",'')");
        }
        if (column.getRef()) {
            sb.append(" AS ");
            sb.append(column.getColumn().getTable().getName());
            sb.append(column.getColumn().getName());
        }
    }

    @Override
    public void visit(OceanBaseUnaryPostfixOperation op) {
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
    public void visit(OceanBaseComputableFunction f) {
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
    public void visit(OceanBaseBinaryLogicalOperation op) {
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
    public void visit(OceanBaseBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(OceanBaseCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ");
        sb.append(op.getType());
        sb.append(")");
    }

    @Override
    public void visit(OceanBaseInOperation op) {
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
    public void visit(OceanBaseOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == OceanBaseOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(OceanBaseExists op) {
        sb.append(" EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(OceanBaseStringExpression op) {
        if (op.getStr().contains("SELECT")) {
            sb.append(op.getStr());
        } else {
            String str = op.getStr();
            if (!str.isEmpty()) {
                sb.append(r.getInteger(0, 100000));
            } else {
                sb.append(r.getInteger(0, 1000000));
            }
        }
    }

    public void visit(OceanBaseStringExpression op, int type) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(OceanBaseTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(OceanBaseAggregate aggr) {
        sb.append(aggr.getAggr());
        sb.append("(");
        visit(aggr.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(OceanBaseColumnName c) {
        sb.append(c.getColumn().getName());
    }

    @Override
    public void visit(OceanBaseText func) {
        visit(func.getExpr());
        sb.append(func.getText());
    }

    @Override
    public void visit(OceanBaseUnaryPrefixOperation op) {
        sb.append("(");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" ");
        visit(op.getExpr());
        sb.append(")");
    }

    // 在 OceanBaseToStringVisitor.java 中添加这个方法

    @Override
    public void visit(OceanBaseJoin join) {
        sb.append(" ");
        switch (join.getType()) {
            case NATURAL:
                sb.append("NATURAL ");
                break;
            case INNER:
                sb.append("INNER ");
                break;
            case LEFT:
                sb.append("LEFT ");
                break;
            case RIGHT:
                sb.append("RIGHT ");
                break;
            default:
                throw new AssertionError(join.getType());
        }
        sb.append("JOIN ");
        //sb.append(join.getTable().getName());
        // 2. 递归地访问要 JOIN 的表引用，而不是直接获取名字
        // 这使得它可以处理 JOIN 子查询等复杂情况
        visit(new OceanBaseTableReference(join.getTable()));
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

}
