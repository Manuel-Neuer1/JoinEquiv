package joinequiv.percona.ast;

import joinequiv.percona.PerconaSchema.PerconaTable;

public class PerconaTableReference implements PerconaExpression {

    private final PerconaTable table;

    public PerconaTableReference(PerconaTable table) {
        this.table = table;
    }

    public PerconaTable getTable() {
        return table;
    }

}
