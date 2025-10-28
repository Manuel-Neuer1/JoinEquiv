package joinequiv.percona.ast;

import joinequiv.Randomly;

public class PerconaOrderByTerm implements PerconaExpression {

    private final PerconaOrder order;
    private final PerconaExpression expr;

    public enum PerconaOrder {
        ASC, DESC;

        public static PerconaOrder getRandomOrder() {
            return Randomly.fromOptions(PerconaOrder.values());
        }
    }

    public PerconaOrderByTerm(PerconaExpression expr, PerconaOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public PerconaOrder getOrder() {
        return order;
    }

    public PerconaExpression getExpr() {
        return expr;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
