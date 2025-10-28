package joinequiv.common.gen;

import java.util.List;

import joinequiv.common.ast.newast.Expression;
import joinequiv.common.ast.newast.Join;
import joinequiv.common.ast.newast.Select;
import joinequiv.common.schema.AbstractTable;
import joinequiv.common.schema.AbstractTableColumn;
import joinequiv.common.schema.AbstractTables;

public interface TLPWhereGenerator<S extends Select<J, E, T, C>, J extends Join<E, T, C>, E extends Expression<C>, T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        extends PartitionGenerator<E, C> {

    TLPWhereGenerator<S, J, E, T, C> setTablesAndColumns(AbstractTables<T, C> tables);

    E generateBooleanExpression();

    S generateSelect();

    List<J> getRandomJoinClauses();

    List<E> getTableRefs();

    List<E> generateFetchColumns(boolean shouldCreateDummy);

    List<E> generateOrderBys();
}
