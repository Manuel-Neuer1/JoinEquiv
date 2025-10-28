package joinequiv.mysql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.Randomly;
import joinequiv.mysql.MySQLGlobalState;
import joinequiv.mysql.MySQLSchema.MySQLTables;
import joinequiv.mysql.ast.MySQLConstant;
import joinequiv.mysql.ast.MySQLExpression;
import joinequiv.mysql.ast.MySQLSelect;
import joinequiv.mysql.ast.MySQLTableReference;

public final class MySQLRandomQuerySynthesizer {

    private MySQLRandomQuerySynthesizer() {
    }

    public static MySQLSelect generate(MySQLGlobalState globalState, int nrColumns) {
        MySQLTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(tables.getColumns());
        MySQLSelect select = new MySQLSelect();

        List<MySQLExpression> allColumns = new ArrayList<>();
        List<MySQLExpression> columnsWithoutAggregations = new ArrayList<>();

        boolean hasGeneratedAggregate = false;

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        for (int i = 0; i < nrColumns; i++) {
            if (Randomly.getBoolean()) {
                MySQLExpression expression = gen.generateExpression();
                allColumns.add(expression);
                columnsWithoutAggregations.add(expression);
            } else {
                allColumns.add(gen.generateAggregate());
                hasGeneratedAggregate = true;
            }
        }
        select.setFetchColumns(allColumns);

        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
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
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        return select;
    }

}
