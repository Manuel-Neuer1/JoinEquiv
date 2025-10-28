package joinequiv.percona.ast;

import joinequiv.IgnoreMeException;
import joinequiv.percona.ast.PerconaBinaryComparisonOperation.BinaryComparisonOperator;
import joinequiv.percona.ast.PerconaBinaryLogicalOperation.PerconaBinaryLogicalOperator;

public class PerconaBetweenOperation implements PerconaExpression {

    private final PerconaExpression expr;
    private final PerconaExpression left;
    private final PerconaExpression right;

    public PerconaBetweenOperation(PerconaExpression expr, PerconaExpression left, PerconaExpression right) {
        this.expr = expr;
        this.left = left;
        this.right = right;
    }

    public PerconaExpression getExpr() {
        return expr;
    }

    public PerconaExpression getLeft() {
        return left;
    }

    public PerconaExpression getRight() {
        return right;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaExpression[] arr = { left, right, expr };
        PerconaConstant convertedExpr = PerconaComputableFunction.castToMostGeneralType(expr.getExpectedValue(), arr);
        PerconaConstant convertedLeft = PerconaComputableFunction.castToMostGeneralType(left.getExpectedValue(), arr);
        PerconaConstant convertedRight = PerconaComputableFunction.castToMostGeneralType(right.getExpectedValue(), arr);


        if (convertedLeft.isInt() && convertedLeft.getInt() < 0 || convertedRight.isInt() && convertedRight.getInt() < 0
                || convertedExpr.isInt() && convertedExpr.getInt() < 0) {
            throw new IgnoreMeException();
        }
        PerconaBinaryComparisonOperation leftComparison = new PerconaBinaryComparisonOperation(convertedLeft, convertedExpr,
                BinaryComparisonOperator.LESS_EQUALS);
        PerconaBinaryComparisonOperation rightComparison = new PerconaBinaryComparisonOperation(convertedExpr,
                convertedRight, BinaryComparisonOperator.LESS_EQUALS);
        PerconaBinaryLogicalOperation andOperation = new PerconaBinaryLogicalOperation(leftComparison, rightComparison,
                PerconaBinaryLogicalOperator.AND);
        return andOperation.getExpectedValue();
    }

}
