// 文件名: CockroachDBDQPOracle.java
package sqlancer.cockroachdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.*;
import sqlancer.cockroachdb.ast.CockroachDBJoin.JoinType;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oceanbase.ast.OceanBaseSelect;

public class CockroachDBDQPOracle implements TestOracle<CockroachDBGlobalState> {

    private final CockroachDBGlobalState state;
    private CockroachDBExpressionGenerator gen;
    private final ExpectedErrors errors = new ExpectedErrors();

    public CockroachDBDQPOracle(CockroachDBGlobalState globalState) {
        this.state = globalState;
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // 1. 生成一个基础的 SELECT 查询，并且必须包含 JOIN
        CockroachDBTables tables = state.getSchema().getRandomTableNonEmptyTables(2); // 确保至少有两张表
        List<CockroachDBTableReference> tableRefs = tables.getTables().stream()
                .map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());

        if (tableRefs.size() != 2) {
            return; // 防御性编程，确保我们只处理两张表
        }

        gen = new CockroachDBExpressionGenerator(state).setColumns(tables.getColumns());

        // 2. 构建查询 (AST)
        CockroachDBSelect select = new CockroachDBSelect();

        // 关键：为了使用 INTERSECT，我们强制使用 DISTINCT
        select.setDistinct(Randomly.getBoolean());

        // 选择要查询的列
        List<CockroachDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        select.setFetchColumns(fetchColumns);
        // **修复 JOIN 的生成逻辑**
        // a. 设置 FROM 子句的基础表
        select.setFromList(List.of(tableRefs.get(0)));

        // b. 生成链式 JOIN
        List<CockroachDBExpression> joinExpressions = new ArrayList<>();
        CockroachDBTableReference leftTable = tableRefs.get(0);
//        for (int i = 1; i < tableRefs.size(); i++) {
            CockroachDBTableReference rightTable = tableRefs.get(1);
            // 关键：我们只测试 INNER JOIN 的等价变换
            CockroachDBJoin join = CockroachDBJoin.createJoin(leftTable, rightTable, JoinType.NATURAL,
                    gen.generateExpression(CockroachDBDataType.BOOL.get()));
            joinExpressions.add(join);
            //leftTable = rightTable; // 在这个简单的链式JOIN中，下一个JOIN的左表是前一个的右表
//        }
        select.setJoinList(joinExpressions);

        // 随机添加 WHERE 子句
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }

        // 3. 获取原始查询 (Q) 的结果
        String originalQueryString = CockroachDBVisitor.asString(select);

        // 如果原始查询没有生成 JOIN，则跳过本次测试
        if (!originalQueryString.toUpperCase().contains("JOIN")) {
            return;
        }

        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // 4. 构建等价查询 (Q')
        // Q' = (Q with LEFT JOIN) INTERSECT (Q with RIGHT JOIN)
        String leftJoinQuery = originalQueryString.replaceAll("(?i)NATURAL JOIN", "NATURAL LEFT JOIN");
        String rightJoinQuery = originalQueryString.replaceAll("(?i)NATURAL JOIN", "NATURAL RIGHT JOIN");

        // CockroachDB 不支持在 INTERSECT 的子查询中使用 ORDER BY，所以需要确保生成的查询没有 ORDER BY
        // 我们的生成器目前没有加 ORDER BY，所以是安全的。

        String equivalentQueryString = "";
        String equivalentQueryString1 = "";
        if (select.isDistinct()) {
            equivalentQueryString = String.format("(%s) INTERSECT (%s)", leftJoinQuery, rightJoinQuery);
        }else{
            equivalentQueryString = String.format("(%s) INTERSECT ALL (%s)", leftJoinQuery, rightJoinQuery);
        }
        if (select.isDistinct()) {
            equivalentQueryString1 = "(" + leftJoinQuery + ")" + "\n EXCEPT \n" + "(" + "(" + leftJoinQuery + ")"+ "\n EXCEPT \n" +"(" + rightJoinQuery + ")" + ")";
        }
        else {
            equivalentQueryString1 = "(" + leftJoinQuery + ")" + "\n EXCEPT ALL\n" + "(" + "(" + leftJoinQuery + ")"+ "\n EXCEPT ALL\n" +"(" + rightJoinQuery + ")" + ")";
        }



        // 5. 获取等价查询 (Q') 的结果
        List<String> equivalentResult = ComparatorHelper.getResultSetFirstColumnAsString(equivalentQueryString, errors, state);
        List<String> equivalentResult1 = ComparatorHelper.getResultSetFirstColumnAsString(equivalentQueryString1, errors, state);

        // 6. 比较结果集
        ComparatorHelper.assumeResultSetsAreEqual(originalResult, equivalentResult, originalQueryString, List.of(equivalentQueryString), state);
        ComparatorHelper.assumeResultSetsAreEqual(originalResult, equivalentResult1, originalQueryString, List.of(equivalentQueryString1), state);
    }
}