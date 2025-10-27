package sqlancer.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBText;
import sqlancer.tidb.gen.TiDBHintGenerator;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBJOINOracle implements TestOracle<TiDBGlobalState> {
    private TiDBExpressionGenerator gen;
    private final TiDBGlobalState state;
    private TiDBSelect select;
    private TiDBSelect TLPselect;
    private final ExpectedErrors errors = new ExpectedErrors();

    public TiDBJOINOracle(TiDBGlobalState globalState) {
        state = globalState;
        TiDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {// Randomly generate a query
        TiDBTables tables = state.getSchema().getRandomTableNonEmptyTables();
        if (tables.getTables().size() > 2) {
            // 我们有足够的表（2个或更多），所以从中精确地随机挑选两个。
            List<TiDBSchema.TiDBTable> fetchedTables = new ArrayList<>(tables.getTables());
            Collections.shuffle(fetchedTables); // 把列表打乱
            List<TiDBSchema.TiDBTable> twoTables = fetchedTables.subList(0, 2); // 取前两个
            tables = new TiDBTables(twoTables); // 用这两个表创建一个新的TiDBTables对象
        }
        gen = new TiDBExpressionGenerator(state).setColumns(tables.getColumns());
        select = new TiDBSelect();

        List<TiDBExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream().map(c -> new TiDBColumnReference(c))
                .collect(Collectors.toList()));
        select.setFetchColumns(fetchColumns);

        List<TiDBExpression> tableList = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        List<TiDBExpression> joins = TiDBJoin.getJoins(tableList, state).stream().collect(Collectors.toList());
        select.setJoinList(joins);
        select.setFromList(tableList);
        TiDBExpression ex = gen.generateExpression();
        if (false) {
            select.setWhereClause(ex);
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(gen.generateExpression());
        }

        String originalQueryString = TiDBVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

        TLPselect = new TiDBSelect(select);

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");
        if (!firstQueryString.equals(originalQueryString)) {
            // 关键：确保使用了 String.format 和括号 ()

            String QueryStringSJT = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);
            String QueryStringADT = "(" + firstQueryString + ")" + " EXCEPT " + "(" + "(" + firstQueryString + ")"+ " EXCEPT " +"(" + secondQueryString + ")" + ")";
            String QueryStringSDT = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
            List<String> ResultSJT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSJT, errors, state);
            List<String> ResultADT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringADT, errors, state);
            List<String> ResultSDT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSDT, errors, state);
//            String QueryString1 = "";
//            if(select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCT || select.getSelectType() == MariaDBSelectStatement.MariaDBSelectType.DISTINCTROW) {
//                QueryString1 = "(" + firstQueryString + ")" + " EXCEPT " + "(" + "(" + firstQueryString + ")"+ " EXCEPT " +"(" + secondQueryString + ")" + ")";
//            } else {
//                QueryString1 = "(" + firstQueryString + ")" + " EXCEPT ALL " + "(" + "(" + firstQueryString + ")"+ " EXCEPT ALL " +"(" + secondQueryString + ")" + ")";
//            }
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSJT, originalQueryString, List.of(QueryStringSJT), state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultADT, originalQueryString, List.of(QueryStringADT), state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSDT, originalQueryString, List.of(QueryStringSDT), state);
            //使用 (A UNION B) EXCEPT ((A EXCEPT B) UNION (B EXCEPT A)) = A INTERSECT B
//            String QueryString1 = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
//            List<String> Result2 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString1, errors, state);
//            ComparatorHelper.assumeResultSetsAreEqual(Result, Result2, QueryString, List.of(QueryString1), state);

//            String QueryString1 = "(" + firstQueryString + ")" + " EXCEPT " + "(" + "(" + firstQueryString + ")"+ " EXCEPT " +"(" + secondQueryString + ")" + ")";
//            List<String> Result1 = ComparatorHelper.getResultSetFirstColumnAsString(QueryString1, errors, state);
//            ComparatorHelper.assumeResultSetsAreEqual(originalResult, Result1, originalQueryString, List.of(QueryString1), state);


            List<TiDBText> hintList = TiDBHintGenerator.generateAllHints(select, tables.getTables());
            for (TiDBText hint : hintList) {
                select.setHint(hint);
                String queryString = TiDBVisitor.asString(select);
                List<String> result = ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, state);
                    //Result = ComparatorHelper.getResultSetFirstColumnAsString(QueryString, errors, state);
                    ComparatorHelper.assumeResultSetsAreEqualforDQP(originalResult, result, queryString, List.of(queryString),
                            state);
            }

            // 方式二：TLP——WHERE
            TLPselect.setWhereClause(ex);
            String tlpFirstQueryString = originalQueryString;
            TLPselect.setWhereClause(gen.negatePredicate(ex));
            String tlpSecondQueryString = TLPselect.asString();
            TLPselect.setWhereClause(gen.isNull(ex));
            String tlpThirdQueryString = TLPselect.asString();


            List<String> combinedString = new ArrayList<>();
            List<String> tlpResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(originalQueryString, tlpSecondQueryString,
                    tlpThirdQueryString, combinedString, true, state, errors);

            ComparatorHelper.assumeResultSetsAreEqualforTLP(originalResult, tlpResultSet, originalQueryString, combinedString,
                    state);
        }


    }

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


