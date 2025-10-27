package sqlancer.mysql.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.sun.jna.platform.unix.X11;
import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
//import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.*;
//import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.gen.MySQLHintGenerator;
//import sqlancer.mysql.gen.MySQLSetGenerator;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;


public class MySQLJOINOracle implements TestOracle<MySQLGlobalState> {
    private final MySQLGlobalState state;
    private MySQLExpressionGenerator gen;
    private MySQLSelect select;
    //private MySQLSelect DQPselect;
    private MySQLSelect TLPselect;
    private final ExpectedErrors errors = new ExpectedErrors();

    public MySQLJOINOracle(MySQLGlobalState globalState) {
        state = globalState;
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        // Randomly generate a query
        MySQLTables tables = state.getSchema().getRandomTableNonEmptyTables(); // 从数据库 schema 中随机选取一个或多个非空的表。结果存储在 tables 对象中
        gen = new MySQLExpressionGenerator(state).setColumns(tables.getColumns()); // 创建一个新的表达式生成器 gen，并告诉它这次查询可用的所有列是 tables.getColumns()。
        List<MySQLExpression> fetchColumns = new ArrayList<>(); // fetchColumns 是一个列表，用来存放 SELECT 后面要查询的列(最终，fetchColumns 包含了 SELECT col1, col2, ... 中的列列表)
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream() // Randomly.nonEmptySubset(tables.getColumns())：从所有可用列中随机选择一个非空的子集
                .map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList())); //.map(c -> new MySQLColumnReference(c, null)): 将每个选中的 MySQLColumn 对象转换成一个 MySQLColumnReference AST 节点

        select = new MySQLSelect(); // 创建一个空的 MySQLSelect AST 对象 select
        select.setFetchColumns(fetchColumns); // 将刚才生成的 fetchColumns 设置为这个查询要获取的列

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values())); // 随机决定 SELECT 的类型，可能是 SELECT、SELECT DISTINCT、SELECT ALL,SELECT DISTINCTROW

        boolean isDistinct = true;
        MySQLExpression ex = gen.generateExpression();
        if (false) { // 以 50% 的概率为查询生成一个随机的 WHERE 子句。gen.generateExpression() 会创建一个复杂的布尔表达式。
            select.setWhereClause(ex);
        }
//        if (Randomly.getBoolean()) { // 以 50% 的概率添加 GROUP BY 子句。这里简单地使用 SELECT 的列作为 GROUP BY 的列。
//            select.setGroupByExpressions(fetchColumns);
//            if (Randomly.getBoolean()) { // 如果添加了 GROUP BY，再以 50% 的概率添加一个随机的 HAVING 子句。
//                select.setHavingClause(gen.generateExpression());
//            }
//        }

        // Set the join. 调用 MySQLJoin.getRandomJoinClauses 来生成一系列随机的 JOIN 子句（可能是 INNER JOIN, LEFT JOIN 等，带有随机的 ON 条件）
        List<MySQLJoin> joinExpressions = MySQLJoin.getRandomJoinClauses(tables.getTables(), state);
        select.setJoinList(joinExpressions.stream().map(j -> (MySQLExpression) j).collect(Collectors.toList())); // 将生成的 JOIN 列表设置到 select 对象中

        // Set the from clause from the tables that are not used in the join.
        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);

        // Get the result of the first query     MySQLVisitor.asString(select): 将我们构建好的 select AST 对象转换成一个具体的 SQL 查询字符串
        String originalQueryString = MySQLVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state); // 如果成功，获取结果集第一列的所有值，并将它们转换成字符串列表 originalResult

        //DQPselect = new MySQLSelect(select);
        TLPselect = new MySQLSelect(select);
//        //-----------------------------------------------------------------
        // 在 check() 方法中

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");

        if (!firstQueryString.equals(originalQueryString)) {
            // 关键：确保使用了 String.format 和括号 ()
            // String QueryString = String.format("(%s) INTERSECT ALL (%s)", firstQueryString, secondQueryString);
            String QueryStringSJT;
            String QueryStringADT;
            String QueryStringSDT = "";

            if(select.getFromOptions() == MySQLSelect.SelectType.DISTINCT || select.getFromOptions() == MySQLSelect.SelectType.DISTINCTROW) {
                isDistinct = true;
                QueryStringSJT = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);
                QueryStringADT = String.format("(%s) EXCEPT ((%s) EXCEPT (%s))", firstQueryString, firstQueryString, secondQueryString);
                QueryStringSDT = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
            }
            else {
                isDistinct = false;
                QueryStringSJT = String.format("(%s) INTERSECT ALL (%s)", firstQueryString, secondQueryString);

                QueryStringADT = String.format("(%s) EXCEPT ALL ((%s) EXCEPT ALL (%s))", firstQueryString, firstQueryString, secondQueryString);

            }

            List<String> ResultSJT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSJT, errors, state);
            List<String> ResultADT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringADT, errors, state);
            List<String> ResultSDT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSDT, errors, state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSJT, originalQueryString, List.of(QueryStringSJT), state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultADT, originalQueryString, List.of(QueryStringADT), state);
            if(isDistinct) ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSDT, originalQueryString, List.of(QueryStringSDT), state);

            // Check hints (方式一：通过添加 Hint 改变执行计划) Hint 是直接在 SQL 中告诉优化器“请尝试用这种方式执行”的指令。
            List<MySQLText> hintList = MySQLHintGenerator.generateAllHints(select, tables.getTables()); //MySQLHintGenerator.generateAllHints(...): 根据当前查询的结构（比如有哪些表，是否用了索引等），生成一个包含所有可用 Hint 的列表。例如，/*+ BKA(t1) */, /*+ NO_MERGE(t2) */
            for (MySQLText hint : hintList) { //遍历所有生成的 Hint。
                select.setHint(hint); // 在每一次循环中，将当前的 hint 设置到 select AST 对象中
                String queryString = MySQLVisitor.asString(select); // 将带有新 Hint 的 select AST 重新转换成 SQL 字符串
                List<String> Hintresult = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state); // 执行这个带有 Hint 的查询，并获取其第一列的结果
                ComparatorHelper.assumeResultSetsAreEqualforDQP(originalResult, Hintresult, originalQueryString, List.of(queryString),
                        state); // 它会比较黄金标准 originalResult 和带有 Hint 的查询结果 result. 如果不相等，它会抛出一个 AssertionError.
            }
            // 方式二：TLP——WHERE
            TLPselect.setWhereClause(ex);
            String tlpFirstQueryString = TLPselect.asString();
            TLPselect.setWhereClause(gen.negatePredicate(ex));
            String tlpSecondQueryString = TLPselect.asString();
            TLPselect.setWhereClause(gen.isNull(ex));
            String tlpThirdQueryString = TLPselect.asString();


            List<String> combinedString = new ArrayList<>();
            List<String> tlpResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(originalQueryString, tlpSecondQueryString,
                    tlpThirdQueryString, combinedString, isDistinct, state, errors);

            ComparatorHelper.assumeResultSetsAreEqualforTLP(originalResult, tlpResultSet, originalQueryString, combinedString,
                    state);
        }
//        // Check optimizer variables 方式二：通过修改 Optimizer Switch 改变执行计划。Optimizer Switch 是通过 SET 命令来全局开启或关闭某个优化特性的会话级变量
//        List<SQLQueryAdapter> optimizationList = MySQLSetGenerator.getAllOptimizer(state); // 生成一个 SQLQueryAdapter 列表。每个 SQLQueryAdapter 都包装了一个 SET 命令，用于修改一个优化器开关。例如 SET @@optimizer_switch='index_merge=off' 或 SET @@optimizer_switch='mrr=on'。
//        for (SQLQueryAdapter optimization : optimizationList) { // 遍历所有 SET 命令
//            optimization.execute(state); // 在数据库会话中执行这个 SET 命令，从而改变当前会话的优化器行为
//            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state); // 再次执行原始的、不带任何 Hint 的查询字符串 originalQueryString（因为我们刚刚修改了会话级的优化器开关，MySQL 在执行这个完全相同的查询字符串时，内部会采用不同的执行计划）
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
