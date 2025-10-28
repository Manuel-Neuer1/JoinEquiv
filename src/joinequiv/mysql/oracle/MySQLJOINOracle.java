package joinequiv.mysql.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.ComparatorHelper;
import joinequiv.Randomly;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
//import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mysql.MySQLErrors;
import joinequiv.mysql.MySQLGlobalState;
import joinequiv.mysql.MySQLSchema.MySQLTables;
import joinequiv.mysql.MySQLVisitor;
import joinequiv.mysql.ast.*;
//import joinequiv.mysql.ast.MySQLText;
import joinequiv.mysql.gen.MySQLExpressionGenerator;
//import joinequiv.mysql.gen.MySQLSetGenerator;



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
        MySQLTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(tables.getColumns());
        List<MySQLExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new MySQLColumnReference(c, null)).collect(Collectors.toList()));

        select = new MySQLSelect();
        select.setFetchColumns(fetchColumns);

        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));

        boolean isDistinct = true;
        MySQLExpression ex = gen.generateExpression();
        if (Randomly.getBoolean()) {
            select.setWhereClause(ex);
        }

        // Set the join.
        List<MySQLJoin> joinExpressions = MySQLJoin.getRandomJoinClauses(tables.getTables(), state);
        select.setJoinList(joinExpressions.stream().map(j -> (MySQLExpression) j).collect(Collectors.toList()));

        // Set the from clause from the tables that are not used in the join.
        List<MySQLExpression> tableList = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);

        String originalQueryString = MySQLVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

//        TLPselect = new MySQLSelect(select);

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");

        if (!firstQueryString.equals(originalQueryString)) {

            String QueryStringSJT;
            String QueryStringADT;
            String QueryStringSDT = "";

            if(select.getFromOptions() == MySQLSelect.SelectType.DISTINCT) {
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

        }

    }

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
