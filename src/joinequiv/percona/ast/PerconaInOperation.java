package joinequiv.percona.ast;

import joinequiv.IgnoreMeException;

import java.util.List;

public class PerconaInOperation implements PerconaExpression {

    private final PerconaExpression expr;
    private final List<PerconaExpression> listElements;
    private final boolean isTrue;

    public PerconaInOperation(PerconaExpression expr, List<PerconaExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public PerconaExpression getExpr() {
        return expr;
    }

    public List<PerconaExpression> getListElements() {
        return listElements;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) {
            return PerconaConstant.createNullConstant();
        }
        if (leftVal.isInt() && !leftVal.isSigned()) {
            throw new IgnoreMeException();
        }

        boolean isNull = false;
        for (PerconaExpression rightExpr : listElements) {
            PerconaConstant rightVal = rightExpr.getExpectedValue();

            if (rightVal.isInt() && !rightVal.isSigned()) {
                throw new IgnoreMeException();
            }
            PerconaConstant convertedRightVal = rightVal;
            PerconaConstant isEquals = leftVal.isEquals(convertedRightVal);
            if (isEquals.isNull()) {
                isNull = true;
            } else {
                if (isEquals.getInt() == 1) {
                    return PerconaConstant.createBoolean(isTrue);
                }
            }
        }
        if (isNull) {
            return PerconaConstant.createNullConstant();
        } else {
            return PerconaConstant.createBoolean(!isTrue);
        }

    }

    public boolean isTrue() {
        return isTrue;
    }
}
