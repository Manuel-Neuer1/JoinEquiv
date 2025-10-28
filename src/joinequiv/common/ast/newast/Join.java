package joinequiv.common.ast.newast;

import joinequiv.common.schema.AbstractTable;
import joinequiv.common.schema.AbstractTableColumn;

public interface Join<E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends Expression<C> {

    void setOnClause(E onClause);
}
