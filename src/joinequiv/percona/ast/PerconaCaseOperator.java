package joinequiv.percona.ast;

import joinequiv.common.ast.newast.NewCaseOperatorNode;

import java.util.List;

public class PerconaCaseOperator extends NewCaseOperatorNode<PerconaExpression> implements PerconaExpression {

    public PerconaCaseOperator(PerconaExpression switchCondition, List<PerconaExpression> whenExprs,
                             List<PerconaExpression> thenExprs, PerconaExpression elseExpr) {
        super(switchCondition, whenExprs, thenExprs, elseExpr);
    }

    @Override
    public PerconaConstant getExpectedValue() {
        int nrConditions = getConditions().size();

        PerconaExpression switchCondition = getSwitchCondition();
        List<PerconaExpression> whenExprs = getConditions();
        List<PerconaExpression> thenExprs = getExpressions();
        PerconaExpression elseExpr = getElseExpr();

        if (switchCondition != null) {
            PerconaConstant switchValue = switchCondition.getExpectedValue();

            for (int i = 0; i < nrConditions; i++) {
                PerconaConstant whenValue = whenExprs.get(i).getExpectedValue();
                PerconaConstant isConditionMatched = switchValue.isEquals(whenValue);
                if (!isConditionMatched.isNull() && isConditionMatched.asBooleanNotNull()) {
                    return thenExprs.get(i).getExpectedValue();
                }
            }
        } else {
            for (int i = 0; i < nrConditions; i++) {
                PerconaConstant whenValue = whenExprs.get(i).getExpectedValue();
                if (!whenValue.isNull() && whenValue.asBooleanNotNull()) {
                    return thenExprs.get(i).getExpectedValue();
                }
            }
        }

        if (elseExpr != null) {
            return elseExpr.getExpectedValue();
        }

        return PerconaConstant.createNullConstant();
    }
}
