package joinequiv.percona.ast;

import joinequiv.common.ast.UnaryNode;

public class PerconaCollate extends UnaryNode<PerconaExpression> implements PerconaExpression {

    private final String collate;

    public PerconaCollate(PerconaExpression expr, String text) {
        super(expr);
        this.collate = text;
    }

    @Override
    public String getOperatorRepresentation() {
        return String.format("COLLATE '%s'", collate);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
