package joinequiv.mariadb.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.ComparatorHelper;
import joinequiv.Randomly;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mariadb.MariaDBErrors;
import joinequiv.mariadb.MariaDBProvider.MariaDBGlobalState;
import joinequiv.mariadb.MariaDBSchema;
import joinequiv.mariadb.MariaDBSchema.MariaDBTables;
import joinequiv.mariadb.ast.MariaDBColumnName;
import joinequiv.mariadb.ast.MariaDBExpression;
import joinequiv.mariadb.ast.MariaDBJoin;
import joinequiv.mariadb.ast.MariaDBSelectStatement;
import joinequiv.mariadb.ast.MariaDBTableReference;
import joinequiv.mariadb.ast.MariaDBVisitor;
import joinequiv.mariadb.gen.MariaDBExpressionGenerator;
import joinequiv.mariadb.gen.MariaDBSetGenerator;

public class MariaDBDQPOracle implements TestOracle<MariaDBGlobalState> {
    private final MariaDBGlobalState state;
    private final MariaDBSchema s;
    private MariaDBExpressionGenerator gen;
    private MariaDBSelectStatement select;
    private final ExpectedErrors errors = new ExpectedErrors();

    public MariaDBDQPOracle(MariaDBGlobalState globalState) {
        state = globalState;
        s = globalState.getSchema();
        MariaDBErrors.addCommonErrors(errors);
    }

    @Override
    public void check() throws Exception {
        MariaDBTables tables = s.getRandomTableNonEmptyTables();
        gen = new MariaDBExpressionGenerator(state.getRandomly()).setColumns(tables.getColumns());

        List<MariaDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new MariaDBColumnName(c))
                .collect(Collectors.toList()));

        select = new MariaDBSelectStatement();
        select.setFetchColumns(fetchColumns);

        select.setSelectType(Randomly.fromOptions(MariaDBSelectStatement.MariaDBSelectType.values()));

        select.setWhereClause(gen.getRandomExpression());



        if (Randomly.getBoolean()) {
            select.setGroupByClause(fetchColumns);
        }

        // Set the join.
        List<MariaDBJoin> joinExpressions = MariaDBJoin.getRandomJoinClauses(tables.getTables(), state.getRandomly());
        select.setJoinClauses(joinExpressions);

        // Set the from clause from the tables that are not used in the join.
        select.setFromList(
                tables.getTables().stream().map(t -> new MariaDBTableReference(t)).collect(Collectors.toList()));

        // Get the result of the first query
        String originalQueryString = MariaDBVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");
        if (!firstQueryString.equals(originalQueryString)) {
            // 关键：确保使用了 String.format 和括号 ()
            String QueryString = "";
            if(select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCT || select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCTROW) {
                QueryString = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);
            } else {
                QueryString = String.format("(%s) INTERSECT ALL (%s)", firstQueryString, secondQueryString);
            }
            List<String> Result = ComparatorHelper.getResultSetFirstColumnAsString(QueryString, errors, state);

            if(select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCT || select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCTROW) {
                String QueryString1 = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
                List<String> Result1 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString1, errors, state);
                ComparatorHelper.assumeResultSetsAreEqual(Result, Result1, QueryString, List.of(QueryString1), state);
            }

        }

        List<SQLQueryAdapter> optimizationList = MariaDBSetGenerator.getAllOptimizer(state);
        for (SQLQueryAdapter optimization : optimizationList) {
            optimization.execute(state);
            List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
            try {
                ComparatorHelper.assumeResultSetsAreEqual(originalResult, result, originalQueryString,
                        List.of(originalQueryString), state);
            } catch (AssertionError e) {
                String assertionMessage = String.format(
                        "The size of the result sets mismatch (%d and %d)!" + System.lineSeparator()
                                + "First query: \"%s\", whose cardinality is: %d" + System.lineSeparator()
                                + "Second query:\"%s\", whose cardinality is: %d",
                        originalResult.size(), result.size(), originalQueryString, originalResult.size(),
                        String.join(";", originalQueryString), result.size());
                assertionMessage += System.lineSeparator() + "The setting: " + optimization.getQueryString();
                throw new AssertionError(assertionMessage);
            }
        }

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
