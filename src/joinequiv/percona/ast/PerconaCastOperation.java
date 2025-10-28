package joinequiv.percona.ast;

import joinequiv.percona.ast.PerconaConstant;
import joinequiv.percona.ast.PerconaExpression;

public class PerconaCastOperation implements PerconaExpression {

    private final PerconaExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
        }

    }

    public PerconaCastOperation(PerconaExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public PerconaExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(type);
    }

}