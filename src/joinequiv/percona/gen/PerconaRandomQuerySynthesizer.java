package joinequiv.percona.gen;

import joinequiv.Randomly;
import joinequiv.percona.PerconaSchema.PerconaTables;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.ast.PerconaConstant;
import joinequiv.percona.ast.PerconaExpression;
import joinequiv.percona.ast.PerconaSelect;
import joinequiv.percona.ast.PerconaTableReference;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class PerconaRandomQuerySynthesizer {

    private PerconaRandomQuerySynthesizer() {
    }

    public static PerconaSelect generate(PerconaGlobalState globalState, int nrColumns) {
        PerconaTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState).setColumns(tables.getColumns());
        PerconaSelect select = new PerconaSelect();

        List<PerconaExpression> allColumns = new ArrayList<>();
        List<PerconaExpression> columnsWithoutAggregations = new ArrayList<>();

        boolean hasGeneratedAggregate = false;

        select.setSelectType(Randomly.fromOptions(PerconaSelect.SelectType.values()));
        for (int i = 0; i < nrColumns; i++) {
            if (Randomly.getBoolean()) {
                PerconaExpression expression = gen.generateExpression();
                allColumns.add(expression);
                columnsWithoutAggregations.add(expression);
            } else {
                allColumns.add(gen.generateAggregate());
                hasGeneratedAggregate = true;
            }
        }
        select.setFetchColumns(allColumns);

        List<PerconaExpression> tableList = tables.getTables().stream().map(t -> new PerconaTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (hasGeneratedAggregate || Randomly.getBoolean()) {
            select.setGroupByExpressions(columnsWithoutAggregations);
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(PerconaConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(PerconaConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

}
