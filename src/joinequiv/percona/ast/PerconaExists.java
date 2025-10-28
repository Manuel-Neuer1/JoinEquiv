package joinequiv.percona.ast;

public class PerconaExists implements PerconaExpression {

    private final PerconaExpression expr;
    private final PerconaConstant expected;

    public PerconaExists(PerconaExpression expr, PerconaConstant expectedValue) {
        this.expr = expr;
        this.expected = expectedValue;
    }

    public PerconaExists(PerconaExpression expr) {
        this.expr = expr;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public PerconaExpression getExpr() {
        return expr;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return expected;
    }

}
