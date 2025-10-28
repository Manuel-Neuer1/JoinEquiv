package joinequiv.tidb.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.Randomly;
import joinequiv.common.gen.ExpressionGenerator;
import joinequiv.common.oracle.TernaryLogicPartitioningOracleBase;
import joinequiv.common.oracle.TestOracle;
import joinequiv.tidb.TiDBErrors;
import joinequiv.tidb.TiDBExpressionGenerator;
import joinequiv.tidb.TiDBProvider.TiDBGlobalState;
import joinequiv.tidb.TiDBSchema;
import joinequiv.tidb.TiDBSchema.TiDBTable;
import joinequiv.tidb.TiDBSchema.TiDBTables;
import joinequiv.tidb.ast.TiDBColumnReference;
import joinequiv.tidb.ast.TiDBExpression;
import joinequiv.tidb.ast.TiDBJoin;
import joinequiv.tidb.ast.TiDBSelect;
import joinequiv.tidb.ast.TiDBTableReference;
import joinequiv.tidb.gen.TiDBHintGenerator;

public abstract class TiDBTLPBase extends TernaryLogicPartitioningOracleBase<TiDBExpression, TiDBGlobalState>
        implements TestOracle<TiDBGlobalState> {

    TiDBSchema s;
    TiDBTables targetTables;
    TiDBExpressionGenerator gen;
    TiDBSelect select;

    public TiDBTLPBase(TiDBGlobalState state) {
        super(state);
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new TiDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new TiDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<TiDBTable> tables = targetTables.getTables();
        if (Randomly.getBoolean()) {
            TiDBHintGenerator.generateHints(select, tables);
        }

        List<TiDBExpression> tableList = tables.stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state).stream().collect(Collectors.toList());
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<TiDBExpression> generateFetchColumns() {
        return Arrays.asList(new TiDBColumnReference(targetTables.getColumns().get(0)));
    }

    @Override
    protected ExpressionGenerator<TiDBExpression> getGen() {
        return gen;
    }

}
