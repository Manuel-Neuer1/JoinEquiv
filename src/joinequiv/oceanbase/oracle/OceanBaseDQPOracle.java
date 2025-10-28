package joinequiv.oceanbase.oracle;



import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.ComparatorHelper;
import joinequiv.Randomly;

import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;

import joinequiv.oceanbase.OceanBaseErrors;
import joinequiv.oceanbase.OceanBaseGlobalState;

//import joinequiv.oceanbase.OceanBaseSchema;
import static joinequiv.oceanbase.OceanBaseSchema.OceanBaseTable; // <-- 添加这个静态导入
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseTables;
import joinequiv.oceanbase.OceanBaseVisitor;
import joinequiv.oceanbase.ast.*;

import joinequiv.oceanbase.gen.OceanBaseExpressionGenerator;


//import joinequiv.OceanBase.gen.OceanBaseSetGenerator;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;


public class OceanBaseDQPOracle implements TestOracle<OceanBaseGlobalState> {
    private final OceanBaseGlobalState state;
    private OceanBaseExpressionGenerator gen;
    private OceanBaseSelect select;
    private final ExpectedErrors errors = new ExpectedErrors();

    public OceanBaseDQPOracle(OceanBaseGlobalState globalState){
        state = globalState;
        OceanBaseErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        // Randomly generate a query
        OceanBaseTables tables = state.getSchema().getRandomTableNonEmptyTables(); // 从数据库 schema 中随机选取一个或多个非空的表。结果存储在 tables 对象中
        gen = new OceanBaseExpressionGenerator(state).setColumns(tables.getColumns()); // 创建一个新的表达式生成器 gen，并告诉它这次查询可用的所有列是 tables.getColumns()。
        List<OceanBaseExpression> fetchColumns = new ArrayList<>(); // fetchColumns 是一个列表，用来存放 SELECT 后面要查询的列(最终，fetchColumns 包含了 SELECT col1, col2, ... 中的列列表)
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream() // Randomly.nonEmptySubset(tables.getColumns())：从所有可用列中随机选择一个非空的子集
                .map(c -> new OceanBaseColumnReference(c, null)).collect(Collectors.toList())); //.map(c -> new OceanBaseColumnReference(c, null)): 将每个选中的 OceanBaseColumn 对象转换成一个 OceanBaseColumnReference AST 节点

        select = new OceanBaseSelect(); // 创建一个空的 OceanBaseSelect AST 对象 select
        select.setFetchColumns(fetchColumns); // 将刚才生成的 fetchColumns 设置为这个查询要获取的列

        select.setSelectType(Randomly.fromOptions(OceanBaseSelect.SelectType.values())); // 随机决定 SELECT 的类型，可能是 SELECT、SELECT DISTINCT、SELECT ALL,SELECT DISTINCTROW
        if (true) { // 以 100% 的概率为查询生成一个随机的 WHERE 子句。gen.generateExpression() 会创建一个复杂的布尔表达式。
            select.setWhereClause(gen.generateExpression());

            if (Randomly.getBoolean()) { // 以 50% 的概率添加 GROUP BY 子句。这里简单地使用 SELECT 的列作为 GROUP BY 的列。
                select.setGroupByExpressions(fetchColumns);
//                if (Randomly.getBoolean()) { // 如果添加了 GROUP BY，再以 50% 的概率添加一个随机的 HAVING 子句。
//                    select.setHavingClause(gen.generateExpression());
//                }
            }
        }

        // TODO // [你的旧代码 - 错误的方式] 这段代码是MYSQL的DQP
//        // Set the join. 调用 OceanBaseJoin.getRandomJoinClauses 来生成一系列随机的 JOIN 子句（可能是 INNER JOIN, LEFT JOIN 等，带有随机的 ON 条件）
//        List<OceanBaseJoin> joinExpressions = OceanBaseJoin.getRandomJoinClauses(tables.getTables(), state);
//        select.setJoinList(joinExpressions.stream().map(j -> (OceanBaseExpression) j).collect(Collectors.toList())); // 将生成的 JOIN 列表设置到 select 对象中
//
//        // Set the from clause from the tables that are not used in the join.
//        List<OceanBaseExpression> tableList = tables.getTables().stream().map(t -> new OceanBaseTableReference(t))
//                .collect(Collectors.toList());
//        select.setFromList(tableList);

        // [新的代码 - 正确的方式]

// 1. 获取所有要参与查询的表
        List<OceanBaseTable> selectedTables = tables.getTables();

// 2. 将第一张表设置为 FROM 子句的唯一内容
        List<OceanBaseExpression> fromClause = new ArrayList<>();
        fromClause.add(new OceanBaseTableReference(selectedTables.get(0)));
        select.setFromList(fromClause);

// 3. 将剩余的表用于生成 JOIN 子句
        if (selectedTables.size() > 1) {
            // tablesToJoin 变量实际上可以被 getRandomJoinClauses 内部逻辑替代，所以我们可以先去掉它
            // List<OceanBaseTable> tablesToJoin = selectedTables.subList(1, selectedTables.size()); // 这行可以删掉

            // 将包含了所有被选中表的列表传递给 JOIN 生成器
            List<OceanBaseJoin> joinExpressions = OceanBaseJoin.getRandomJoinClauses(selectedTables, state);

            select.setJoinList(joinExpressions.stream().map(j -> (OceanBaseExpression) j).collect(Collectors.toList()));
        } else {
            // 如果只有一张表，JOIN 列表就是空的
            select.setJoinList(new ArrayList<>());
        }
        /// ////////////////////////////////////////////////////////////////////////////////////////

        // Get the result of the first query     OceanBaseVisitor.asString(select): 将我们构建好的 select AST 对象转换成一个具体的 SQL 查询字符串
        String originalQueryString = OceanBaseVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state); // 如果成功，获取结果集第一列的所有值，并将它们转换成字符串列表 originalResult

//        //-----------------------------------------------------------------
        // 在 check() 方法中

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");

        if (!firstQueryString.equals(originalQueryString)) {
            // 关键：确保使用了 String.format 和括号 ()
            String QueryString = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);
            List<String> Result = ComparatorHelper.getResultSetFirstColumnAsString(QueryString, errors, state);
            String QueryString1 = "";
            if(select.getFromOptions() == OceanBaseSelect.SelectType.DISTINCT) QueryString1 = "(" + firstQueryString + ")" + "\n EXCEPT \n" + "(" + "(" + firstQueryString + ")"+ "\n EXCEPT \n" +"(" + secondQueryString + ")" + ")";
            else QueryString1 = "(" + firstQueryString + ")" + "\n EXCEPT ALL\n" + "(" + "(" + firstQueryString + ")"+ "\n EXCEPT ALL\n" +"(" + secondQueryString + ")" + ")";
            List<String> Result1 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString1, errors, state);


            String QueryString2 = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
            List<String> Result2 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString2, errors, state);
            ComparatorHelper.assumeResultSetsAreEqual(Result, Result2, QueryString, List.of(QueryString2), state);

            ComparatorHelper.assumeResultSetsAreEqual(originalResult, Result, originalQueryString, List.of(QueryString1),
                    state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, Result1, originalQueryString, List.of(QueryString1),
                    state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, Result2, originalQueryString, List.of(QueryString2),
                    state);

        }

//        String QueryString111 = "SELECT 1 WHERE FALSE";
//        List<String> Result111 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString111, errors, state);
//        String QueryString222 = String.format("(%s) INTERSECT (%s)", originalQueryString, originalQueryString);
//        List<String> Result222 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString222, errors, state);
//        String QueryString333 = String.format("(%s) EXCEPT (%s)", originalQueryString, originalQueryString);
//        List<String> Result333 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString333, errors, state);
//        ComparatorHelper.assumeResultSetsAreEqual(Result111, Result333, QueryString111, List.of(QueryString333),
//                state);
//        ComparatorHelper.assumeResultSetsAreEqual(originalResult, Result222, originalQueryString, List.of(QueryString222),
//                state);
//        // 1. 找到 FROM 的位置
//        int fromIndex = originalQueryString.toUpperCase().indexOf(" FROM ");
//        if (fromIndex == -1) {
//            return; // 无法处理
//        }
//
//// 2. 分离 SELECT 和 FROM 之后的部分
//        String selectClause = originalQueryString.substring(0, fromIndex); // e.g., "SELECT DISTINCTROW t0.c0 AS ref0"
//        String fromAndRest = originalQueryString.substring(fromIndex + " FROM ".length()); // "t0 NATURAL JOIN t1 WHERE ..."
//
//// 3. 找到 WHERE/GROUP BY/HAVING/... 的起始位置（保留后缀）
//        Pattern clausePattern = Pattern.compile("(?i)\\bWHERE\\b|\\bGROUP BY\\b|\\bHAVING\\b|\\bORDER BY\\b|\\bLIMIT\\b");
//        Matcher matcher = clausePattern.matcher(fromAndRest);
//        int suffixStart = -1;
//        if (matcher.find()) {
//            suffixStart = matcher.start();
//        }
//
//// 4. 拆分出 JOIN 子句与后缀
//        String baseFromClause = (suffixStart == -1) ? fromAndRest : fromAndRest.substring(0, suffixStart);
//        String suffixClause = (suffixStart == -1) ? "" : fromAndRest.substring(suffixStart);
//
//// 5. 构造 LEFT JOIN 和 RIGHT JOIN 子句
////        String leftJoinClause = baseFromClause.replaceAll("(?i)NATURAL JOIN", "NATURAL LEFT JOIN");
////        String rightJoinClause = baseFromClause.replaceAll("(?i)NATURAL JOIN", "NATURAL RIGHT JOIN");
//
//        // 正则表达式，匹配 "NATURAL JOIN", "INNER JOIN", 或单独的 "JOIN" (当它不被 LEFT/RIGHT/OUTER/CROSS 修饰时)
//        // (?i) 表示不区分大小写
//        // \\b 表示单词边界，防止匹配到像 "WINNER" 这样的词
//        // (?:...) 表示一个非捕获组
//        // (?!...) 表示一个负向先行断言，确保 JOIN 前面没有 LEFT/RIGHT/FULL/CROSS
//        // 简化的正则表达式，只匹配 "NATURAL JOIN" 或 "INNER JOIN"
//        String joinPattern = "(?i)\\b(NATURAL|INNER)\\s+JOIN\\b";
//
//        // 替换为 LEFT JOIN
//        String leftJoinClause = baseFromClause.replaceAll(joinPattern, "LEFT JOIN");
//
//        // 替换为 RIGHT JOIN
//        String rightJoinClause = baseFromClause.replaceAll(joinPattern, "RIGHT JOIN");
//
//
//// 若没替换成功，表示不是 NATURAL/INNER JOIN，跳过
//        if (leftJoinClause.equals(baseFromClause)) {
//            return;
//        }
//
//// ✅ 去除 SELECT 和 GROUP BY 中的表名前缀（解决 Unknown column 't0.c0' 问题）
//        selectClause = selectClause.replaceAll("\\b[a-zA-Z_][a-zA-Z0-9_]*\\.", "");
//        suffixClause = suffixClause.replaceAll("\\b[a-zA-Z_][a-zA-Z0-9_]*\\.", "");
//
//// 6. 构造等价 INNER JOIN 的完整 SQL
//        String innerJoinEquivalent = String.format(
//                "%s FROM ( " +
//                        "SELECT * FROM (SELECT * FROM %s) AS LEFT_ALL " +
//                        "EXCEPT " +
//                        "SELECT * FROM ( " +
//                        "SELECT * FROM (SELECT * FROM %s) AS LEFT_ALL_INNER " +
//                        "EXCEPT " +
//                        "SELECT * FROM (SELECT * FROM %s) AS RIGHT_ALL_INNER " +
//                        ") AS LEFT_ONLY " +
//                        ") AS T_INNER %s",
//                selectClause,
//                leftJoinClause,
//                leftJoinClause,
//                rightJoinClause,
//                suffixClause
//        );
//
//        List<String> Result = ComparatorHelper.getResultSetFirstColumnAsString(innerJoinEquivalent, errors, state);
//        ComparatorHelper.assumeResultSetsAreEqual(
//                originalResult,
//                Result,
//                originalQueryString,
//                List.of(innerJoinEquivalent),
//                state
//        );
//        -----------------------------------------------------------------

//        // Check hints (方式一：通过添加 Hint 改变执行计划) Hint 是直接在 SQL 中告诉优化器“请尝试用这种方式执行”的指令。
//        List<OceanBaseText> hintList = OceanBaseHintGenerator.generateAllHints(select, tables.getTables()); //OceanBaseHintGenerator.generateAllHints(...): 根据当前查询的结构（比如有哪些表，是否用了索引等），生成一个包含所有可用 Hint 的列表。例如，/*+ BKA(t1) */, /*+ NO_MERGE(t2) */
//        for (OceanBaseText hint : hintList) { //遍历所有生成的 Hint。
//            select.setHint(hint); // 在每一次循环中，将当前的 hint 设置到 select AST 对象中
//            String queryString = OceanBaseVisitor.asString(select); // 将带有新 Hint 的 select AST 重新转换成 SQL 字符串
//            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state); // 执行这个带有 Hint 的查询，并获取其第一列的结果
//            ComparatorHelper.assumeResultSetsAreEqual(originalResult, result, originalQueryString, List.of(queryString),
//                    state); // 它会比较黄金标准 originalResult 和带有 Hint 的查询结果 result. 如果不相等，它会抛出一个 AssertionError.
//        }
//
//        // Check optimizer variables 方式二：通过修改 Optimizer Switch 改变执行计划。Optimizer Switch 是通过 SET 命令来全局开启或关闭某个优化特性的会话级变量
//        List<SQLQueryAdapter> optimizationList = OceanBaseSetGenerator.getAllOptimizer(state); // 生成一个 SQLQueryAdapter 列表。每个 SQLQueryAdapter 都包装了一个 SET 命令，用于修改一个优化器开关。例如 SET @@optimizer_switch='index_merge=off' 或 SET @@optimizer_switch='mrr=on'。
//        for (SQLQueryAdapter optimization : optimizationList) { // 遍历所有 SET 命令
//            optimization.execute(state); // 在数据库会话中执行这个 SET 命令，从而改变当前会话的优化器行为
//            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state); // 再次执行原始的、不带任何 Hint 的查询字符串 originalQueryString（因为我们刚刚修改了会话级的优化器开关，OceanBase 在执行这个完全相同的查询字符串时，内部会采用不同的执行计划）
//            try {
//                ComparatorHelper.assumeResultSetsAreEqual(originalResult, result, originalQueryString,
//                        List.of(originalQueryString), state);
//            } catch (AssertionError e) {
//                String assertionMessage = String.format(
//                        "The size of the result sets mismatch (%d and %d)!" + System.lineSeparator()
//                                + "First query: \"%s\", whose cardinality is: %d" + System.lineSeparator()
//                                + "Second query:\"%s\", whose cardinality is: %d",
//                        originalResult.size(), result.size(), originalQueryString, originalResult.size(),
//                        String.join(";", originalQueryString), result.size());
//                assertionMessage += System.lineSeparator() + "The setting: " + optimization.getQueryString();
//                throw new AssertionError(assertionMessage);
//            }
//        }
    }

    /**
     * 根据不同的 JOIN 类型，将它们替换为不同的指定字符串。
     * - "NATURAL JOIN" (不区分大小写，中间可有多个空格) 会被替换为 str1。
     * - "INNER JOIN" (不区分大小写，中间可有多个空格) 会被替换为 str2。
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

        // 步骤1: 替换 NATURAL JOIN
        if (str1 != null) {
            // 正则表达式解释:
            // (?i)       : 不区分大小写模式。
            // \\bNATURAL\\b : 匹配独立的单词 "NATURAL"。
            // [ ]+       : 匹配 "NATURAL" 和 "JOIN" 之间的一个或多个空格。
            //              注意：这里用 [ ]+ 而不是 \\s+，因为 \\s+ 会匹配换行符等，
            //              而你的要求是“只能存在空格”。
            // \\bJOIN\\b   : 匹配独立的单词 "JOIN"。
            String naturalJoinRegex = "(?i)\\bNATURAL[ ]+\\bJOIN\\b";
            result = result.replaceAll(naturalJoinRegex, str1);
        }

        // 步骤2: 在上一步的结果上，替换 INNER JOIN
        if (str2 != null) {
            String innerJoinRegex = "(?i)\\bINNER[ ]+\\bJOIN\\b";
            result = result.replaceAll(innerJoinRegex, str2);
        }

        return result;
    }

}
