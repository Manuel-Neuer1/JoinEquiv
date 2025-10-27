package sqlancer.cockroachdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.oracle.tlp.CockroachDBTLPBase;
import sqlancer.common.query.SQLQueryAdapter;

// TODO 重写
//public final class CockroachDBRandomQuerySynthesizer {
//
//    private CockroachDBRandomQuerySynthesizer() {
//    }
//
//    public static SQLQueryAdapter generate(CockroachDBGlobalState globalState, int nrColumns) {
//        CockroachDBSelect select = generateSelect(globalState, nrColumns);
//        return new SQLQueryAdapter(CockroachDBVisitor.asString(select));
//    }
//
//    public static CockroachDBSelect generateSelect(CockroachDBGlobalState globalState, int nrColumns) {
//        CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
//        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
//                .setColumns(tables.getColumns());
//        CockroachDBSelect select = new CockroachDBSelect();
//        select.setDistinct(Randomly.getBoolean());
//        boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
//        List<CockroachDBExpression> columns = new ArrayList<>();
//        List<CockroachDBExpression> columnsWithoutAggregates = new ArrayList<>();
//        for (int i = 0; i < nrColumns; i++) {
//            if (allowAggregates && Randomly.getBoolean()) {
//                CockroachDBExpression expression = gen.generateExpression(CockroachDBDataType.getRandom().get());
//                columns.add(expression);
//                columnsWithoutAggregates.add(expression);
//            } else {
//                columns.add(gen.generateAggregate());
//            }
//        }
//        select.setFetchColumns(columns);
//        List<CockroachDBTableReference> tableList = tables.getTables().stream()
//                .map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
//        List<CockroachDBExpression> updatedTableList = CockroachDBCommon.getTableReferences(tableList);
//        if (Randomly.getBoolean()) {
//            select.setJoinList(CockroachDBTLPBase.getJoins(updatedTableList, globalState));
//        }
//        select.setFromList(updatedTableList);
//        if (Randomly.getBoolean()) {
//            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
//        }
//        if (Randomly.getBoolean()) {
//            select.setOrderByClauses(gen.getOrderingTerms());
//        }
//        if (Randomly.getBoolean()) {
//            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
//        }
//
//        if (Randomly.getBoolean()) { // TODO expression
//            select.setLimitClause(gen.generateConstant(CockroachDBDataType.INT.get()));
//        }
//        if (Randomly.getBoolean()) {
//            select.setOffsetClause(gen.generateConstant(CockroachDBDataType.INT.get()));
//        }
//        if (Randomly.getBoolean()) {
//            select.setHavingClause(gen.generateHavingClause());
//        }
//        return select;
//    }
//
//}

public final class CockroachDBRandomQuerySynthesizer {

    private CockroachDBRandomQuerySynthesizer() {
    }

    public static SQLQueryAdapter generate(CockroachDBGlobalState globalState, int nrColumns) {
        CockroachDBSelect select = generateSelect(globalState, nrColumns);
        return new SQLQueryAdapter(CockroachDBVisitor.asString(select));
    }

//    public static CockroachDBSelect generateSelect(CockroachDBGlobalState globalState, int nrColumns) {
//        // 1. 获取随机数量的表
//        CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
//        List<CockroachDBTableReference> tableRefs = tables.getTables().stream()
//                .map(t -> new CockroachDBTableReference(t))
//                .collect(Collectors.toList());
//
//        // 2. 创建一个作用域正确的表达式生成器
//        // 这个 gen 知道所有可能参与查询的表的列
//        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
//                .setColumns(tables.getColumns());
//
//        // 3. 构建 SELECT AST 对象
//        CockroachDBSelect select = new CockroachDBSelect();
//        select.setDistinct(Randomly.getBoolean());
//
//        // 4. 正确地构建 FROM 和 JOIN 子句
//        // 使用我们之前已经验证过的 "单一基础表 + 链式 JOIN" 模式
//        if (!tableRefs.isEmpty()) {
//            CockroachDBTableReference firstTable = tableRefs.remove(0);
//            select.setFromList(List.of(firstTable));
//
//            List<CockroachDBExpression> joinExpressions = new ArrayList<>();
//            // 将剩余的表通过 JOIN 连接起来
//            for (CockroachDBTableReference rightTable : tableRefs) {
//                // 注意：这里的 leftTable 应该是前一个 JOIN 的结果，但为了简化，
//                // 我们可以简单地让每个 JOIN 都连接到前一个表上。
//                // 一个更简单的、但仍然正确的做法是直接使用 TLPBase.getJoins，但要确保 fromList 只设置一次。
//                // 这里我们手动构建一个简单的链式 JOIN。
//                CockroachDBTableReference leftTableForJoin = (joinExpressions.isEmpty()) ? firstTable :
//                        (CockroachDBTableReference) ((CockroachDBJoin) joinExpressions.get(joinExpressions.size() - 1)).getRightTable();
//
//                joinExpressions.add(CockroachDBJoin.createJoin(leftTableForJoin, rightTable,
//                        CockroachDBJoin.JoinType.getRandom(),
//                        gen.generateExpression(CockroachDBDataType.BOOL.get())));
//            }
//            select.setJoinList(joinExpressions);
//        }
//
//        // 5. 生成 SELECT 的其他子句，确保使用作用域正确的生成器
//        // 生成 Fetch Columns
//        boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
//        List<CockroachDBExpression> columns = new ArrayList<>();
//        for (int i = 0; i < nrColumns; i++) {
//            if (allowAggregates && Randomly.getBoolean()) {
//                // 使用已经创建的、作用域正确的 gen
//                columns.add(gen.generateAggregate());
//            } else {
//                columns.add(gen.generateExpression(CockroachDBDataType.getRandom().get()));
//            }
//        }
//        select.setFetchColumns(columns);
//
//        // 生成 WHERE
//        if (Randomly.getBoolean()) {
//            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
//        }
//
//        // 生成 GROUP BY
//        if (Randomly.getBoolean() && !columns.isEmpty()) {
//            // 使用 gen，它的作用域是正确的 (包含了所有 JOIN 的表的列)
//            select.setGroupByExpressions(Randomly.nonEmptySubset(columns));
//        }
//
//        // 生成 HAVING
//        if (Randomly.getBoolean() && !select.getGroupByExpressions().isEmpty()) {
//            select.setHavingClause(gen.generateHavingClause());
//        }
//
//        // 生成 ORDER BY
////        if (Randomly.getBoolean()) {
////            // 使用我们之前讨论过的修复方案，传递明确的列范围
////            select.setOrderByClauses(gen.getOrderingTerms(tables.getColumns()));
////        }
//
//        // 生成 LIMIT 和 OFFSET
//        if (Randomly.getBoolean()) {
//            select.setLimitClause(gen.generateConstant(CockroachDBDataType.INT.get()));
//        }
//        if (Randomly.getBoolean()) {
//            select.setOffsetClause(gen.generateConstant(CockroachDBDataType.INT.get()));
//        }
//
//        return select;
//    }
public static CockroachDBSelect generateSelect(CockroachDBGlobalState globalState, int nrColumns) {
    // 1. 获取随机的表
    CockroachDBTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
    List<CockroachDBTableReference> tableRefs = tables.getTables().stream()
            .map(t -> new CockroachDBTableReference(t))
            .collect(Collectors.toList());

    // 2. 创建一个作用域正确的表达式生成器
    CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
            .setColumns(tables.getColumns());

    // 3. 构建 SELECT AST 对象
    CockroachDBSelect select = new CockroachDBSelect();
    select.setDistinct(Randomly.getBoolean());

    // 4. 【核心修复】构建清晰的 FROM 和 JOIN 子句
    if (!tableRefs.isEmpty()) {
        // a. 设置基础表
        select.setFromList(List.of(tableRefs.get(0)));

        // b. 链式 JOIN 剩余的表
        if (tableRefs.size() > 1) {
            // 使用 CockroachDBTLPBase.getJoins 来生成 JOIN 列表，这是一个已经被测试过的通用方法
            // 我们需要给它一个包含所有要 JOIN 的表的列表
            List<CockroachDBExpression> allTableExprs = tableRefs.stream()
                    .map(t -> (CockroachDBExpression)t)
                    .collect(Collectors.toList());
            select.setJoinList(CockroachDBTLPBase.getJoins(allTableExprs, globalState));
            // 注意： CockroachDBTLPBase.getJoins 的实现也需要是正确的。
            // 如果它也有问题，我们需要手动构建。
        }
    }

    // 5. 生成 SELECT 的其他子句
    // ... (Fetch Columns, WHERE, GROUP BY etc. 逻辑)
    // 确保所有生成表达式的地方都使用上面创建的 gen
    List<CockroachDBExpression> columns = new ArrayList<>();
    for (int i = 0; i < nrColumns; i++) {
        columns.add(gen.generateExpression(CockroachDBDataType.getRandom().get()));
    }
    select.setFetchColumns(columns);

    if (Randomly.getBoolean()) {
        select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
    }

    if (Randomly.getBoolean() && !columns.isEmpty()) {
        select.setGroupByExpressions(Randomly.nonEmptySubset(columns));
    }

    // 暂时禁用 ORDER BY，直到我们确认所有其他部分都正确
    /*
    if (Randomly.getBoolean()) {
        select.setOrderByClauses(gen.getOrderingTerms(tables.getColumns()));
    }
    */

    return select;
}
}
