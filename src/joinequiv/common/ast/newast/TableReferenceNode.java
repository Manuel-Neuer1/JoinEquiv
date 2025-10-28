package joinequiv.common.ast.newast;

import joinequiv.common.schema.AbstractTable;

public class TableReferenceNode<E, T extends AbstractTable<?, ?, ?>> {

    private final T t;

    public TableReferenceNode(T table) {
        this.t = table;
    }

    public T getTable() {
        return t;
    }

}
