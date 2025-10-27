package sqlancer.tidb.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.visitor.TiDBVisitor;
import sqlancer.tidb.TiDBSchema.TiDBTable; // 可能需要这个导入

public final class TiDBRandomQuerySynthesizer {

    private TiDBRandomQuerySynthesizer() {
    }

    public static SQLQueryAdapter generate(TiDBGlobalState globalState, int nrColumns) {
        TiDBSelect select = generateSelect(globalState, nrColumns);
        return new SQLQueryAdapter(TiDBVisitor.asString(select));
    }

    public static TiDBSelect generateSelect(TiDBGlobalState globalState, int nrColumns) {
        TiDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        // TODO 这里只生成最多两个表
        if (tables.getTables().size() > 2) {
            // 我们有足够的表（2个或更多），所以从中精确地随机挑选两个。
            List<TiDBTable> fetchedTables = new ArrayList<>(tables.getTables());
            Collections.shuffle(fetchedTables); // 把列表打乱
            List<TiDBTable> twoTables = fetchedTables.subList(0, 2); // 取前两个
            tables = new TiDBTables(twoTables); // 用这两个表创建一个新的TiDBTables对象
        }
        // 至此，无论之前有多少个表，现在的 `tables` 变量要么包含1个表，要么包含2个表。
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(tables.getColumns());
        TiDBSelect select = new TiDBSelect();
        // select.setDistinct(Randomly.getBoolean()); // TODO 我删了


        List<TiDBExpression> columns = new ArrayList<>();
        // TODO: also generate aggregates
        columns.addAll(gen.generateExpressions(nrColumns));
        select.setFetchColumns(columns);
        List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        // TODO: generate joins
        select.setFromList(tableList);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
//        if (Randomly.getBooleanWithRatherLowProbability()) { // TODO 这里是我的修改 不要 Order by 了
//            select.setOrderByClauses(gen.generateOrderBys());
//        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(gen.generateExpression());
        }
        return select;
    }

}
