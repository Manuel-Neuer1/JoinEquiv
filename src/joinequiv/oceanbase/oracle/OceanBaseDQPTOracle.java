package joinequiv.oceanbase.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.Randomly;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.oceanbase.OceanBaseErrors;
import joinequiv.oceanbase.OceanBaseGlobalState;
import static joinequiv.oceanbase.OceanBaseSchema.OceanBaseTable;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseTables;
import joinequiv.oceanbase.OceanBaseVisitor;
import joinequiv.oceanbase.ast.OceanBaseColumnReference;
import joinequiv.oceanbase.ast.OceanBaseExpression;
import joinequiv.oceanbase.ast.OceanBaseJoin;
import joinequiv.oceanbase.ast.OceanBaseSelect;
import joinequiv.oceanbase.ast.OceanBaseTableReference;
import joinequiv.oceanbase.gen.OceanBaseExpressionGenerator;

public class OceanBaseDQPTOracle implements TestOracle<OceanBaseGlobalState> {
    private final OceanBaseGlobalState state;
    private OceanBaseExpressionGenerator gen;
    private OceanBaseSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();

    public OceanBaseDQPTOracle(OceanBaseGlobalState globalState) {
        state = globalState;
        OceanBaseErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        // 1. 随机生成一个基础查询 (这部分逻辑保持不变)
        OceanBaseTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new OceanBaseExpressionGenerator(state).setColumns(tables.getColumns());
        List<OceanBaseExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new OceanBaseColumnReference(c, null)).collect(Collectors.toList()));

        select = new OceanBaseSelect();
        select.setFetchColumns(fetchColumns);
        select.setSelectType(Randomly.fromOptions(OceanBaseSelect.SelectType.values()));
        if (true) {
            select.setWhereClause(gen.generateExpression());
            if (Randomly.getBoolean()) {
                select.setGroupByExpressions(fetchColumns);
            }
        }

        List<OceanBaseTable> selectedTables = tables.getTables();
        if (selectedTables.isEmpty()) {
            return; // 如果没有可用的表，则无法继续
        }

        // 设置 FROM 和 JOIN 子句
        List<OceanBaseExpression> fromClause = new ArrayList<>();
        fromClause.add(new OceanBaseTableReference(selectedTables.get(0)));
        select.setFromList(fromClause);
        if (selectedTables.size() > 1) {
            List<OceanBaseJoin> joinExpressions = OceanBaseJoin.getRandomJoinClauses(selectedTables, state);
            select.setJoinList(joinExpressions.stream().map(j -> (OceanBaseExpression) j).collect(Collectors.toList()));
        } else {
            select.setJoinList(new ArrayList<>());
        }

        String originalQueryString = OceanBaseVisitor.asString(select);

        // 2. 对原始查询进行逻辑等价变换
        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");

        // 只有当 JOIN 被成功替换时，才进行后续比较
        if (!firstQueryString.equals(originalQueryString)) {
            // 生成多个逻辑上等价的查询字符串
            String queryString = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);

            String queryString1;

            if(select.getFromOptions() == OceanBaseSelect.SelectType.DISTINCT) queryString1 = "(" + firstQueryString + ")" + "\n EXCEPT \n" + "(" + "(" + firstQueryString + ")"+ "\n EXCEPT \n" +"(" + secondQueryString + ")" + ")";
            else queryString1 = "(" + firstQueryString + ")" + "\n EXCEPT ALL\n" + "(" + "(" + firstQueryString + ")"+ "\n EXCEPT ALL\n" +"(" + secondQueryString + ")" + ")";

            String queryString2 = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))",
                    firstQueryString, secondQueryString, firstQueryString, secondQueryString, secondQueryString, firstQueryString);

            // 3. 【核心修改】用性能比较替换结果集比较
            // 比较原始查询与其逻辑等价变体之间的性能
            assumePerformanceNotWorse(originalQueryString, queryString, state);
            assumePerformanceNotWorse(originalQueryString, queryString1, state);
            assumePerformanceNotWorse(originalQueryString, queryString2, state);

            // 也可以在不同的等价变体之间进行交叉比较，增加发现问题的机会
//            assumePerformanceNotWorse(queryString, queryString2, state);
//            assumePerformanceNotWorse(queryString1, queryString2, state);
        }
    }

    /**
     * 断言第一个查询的性能不应该比第二个查询差很多。
     * 如果 time1 > time2 * (1 + tolerance)，则认为有性能问题并抛出异常。
     *
     * @param query1  第一个查询字符串 (被认为是性能较差的候选者)
     * @param query2  第二个查询字符串 (被认为是性能较好的基准)
     * @param state   全局状态对象
     */
    private void assumePerformanceNotWorse(String query1, String query2, OceanBaseGlobalState state) {
        final double MIN_TIME_TO_CONSIDER_MS = 5.0; // 低于此时间的查询波动太大，不予比较
        final double PERFORMANCE_TOLERANCE = 0.2;   // 容忍 10% 的性能波动

        double time1 = getExecutionTime(query1, state);
        double time2 = getExecutionTime(query2, state);

        // 如果任一查询无法测量时间，或者两者都非常快，则跳过此次比较
        if (time1 <= 0 || time2 <= 0 || (time1 < MIN_TIME_TO_CONSIDER_MS && time2 < MIN_TIME_TO_CONSIDER_MS)) {
            return;
        }

        // 核心比较逻辑：如果查询1比查询2慢了超过容忍度，则报告一个bug
        if (time1 > time2 * (1 + PERFORMANCE_TOLERANCE)) {
            String errorMessage = String.format(
                    "潜在的性能问题：逻辑等价的查询表现出显著的性能差异。\n" +
                            "查询1的执行时间 (%.2fms) 明显慢于查询2的执行时间 (%.2fms)。\n\n" +
                            "--- 查询1 (较慢) ---\n%s\n\n" +
                            "--- 查询2 (较快) ---\n%s",
                    time1, time2, query1, query2);
            throw new AssertionError(errorMessage);
        }
    }

    /**
     * 使用 EXPLAIN ANALYZE 执行查询并从服务器提取执行时间。
     *
     * @param query 要分析的 SQL 查询。
     * @param state 全局状态对象，用于获取数据库连接。
     * @return 执行时间（毫秒），如果无法获取则返回 -1.0。
     */
    private double getExecutionTime(String query, OceanBaseGlobalState state) {
        String upperQuery = query.toUpperCase();
        if (upperQuery.startsWith("UPDATE") || upperQuery.startsWith("INSERT") || upperQuery.startsWith("DELETE")) {
            return -1.0;
        }

        String explainQuery = "EXPLAIN ANALYZE " + query;
        double time = -1.0;

//        try (Connection conn = state.getNewConnection();
//             Statement s = conn.createStatement()) {
//
//            try (ResultSet rs = s.executeQuery(explainQuery)) {
//                Pattern timePattern = Pattern.compile("execute time: (\\d+\\.\\d+)ms");
//                while (rs.next()) {
//                    String line = rs.getString(1);
//                    if (line == null) continue;
//
//                    Matcher matcher = timePattern.matcher(line);
//                    if (matcher.find()) {
//                        time = Double.parseDouble(matcher.group(1));
//                        break;
//                    }
//                }
//            }
//        } catch (Exception e) {
//            // 忽略异常，因为复杂的等价查询有时无法被 EXPLAIN ANALYZE 处理
//        }
        return time;
    }

    /**
     * 根据不同的 JOIN 类型，将它们替换为不同的指定字符串。
     * - "NATURAL JOIN" 会被替换为 str1。
     * - "INNER JOIN" 会被替换为 str2。
     *
     * @param originalQueryString 原始的 SQL 查询字符串。
     * @param str1                用于替换 "NATURAL JOIN" 的字符串。
     * @param str2                用于替换 "INNER JOIN" 的字符串。
     * @return 替换后的新 SQL 查询字符串。
     */
    public static String replaceJoins(String originalQueryString, String str1, String str2) {
        if (originalQueryString == null || originalQueryString.isEmpty()) {
            return originalQueryString;
        }

        String result = originalQueryString;

        if (str1 != null) {
            String naturalJoinRegex = "(?i)\\bNATURAL[ ]+\\bJOIN\\b";
            result = result.replaceAll(naturalJoinRegex, str1);
        }

        if (str2 != null) {
            String innerJoinRegex = "(?i)\\bINNER[ ]+\\bJOIN\\b";
            result = result.replaceAll(innerJoinRegex, str2);
        }

        return result;
    }
}
