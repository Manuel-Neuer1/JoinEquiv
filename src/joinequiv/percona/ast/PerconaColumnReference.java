package joinequiv.percona.ast;

import joinequiv.percona.PerconaSchema.PerconaColumn;

public class PerconaColumnReference implements PerconaExpression {

    private final PerconaColumn column;
    private final PerconaConstant value;

    public PerconaColumnReference(PerconaColumn column, PerconaConstant value) {
        this.column = column;
        this.value = value;
    }

    public static PerconaColumnReference create(PerconaColumn column, PerconaConstant value) {
        return new PerconaColumnReference(column, value);
    }

    public PerconaColumn getColumn() {
        return column;
    }

    public PerconaConstant getValue() {
        return value;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return value;
    }

}
