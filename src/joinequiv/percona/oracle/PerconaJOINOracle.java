package joinequiv.percona.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.ComparatorHelper;
import joinequiv.Randomly;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
//import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaErrors;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.PerconaSchema.PerconaTables;
import joinequiv.percona.PerconaVisitor;
import joinequiv.percona.ast.*;
//import joinequiv.percona.ast.PerconaText;
import joinequiv.percona.gen.PerconaExpressionGenerator;
//import joinequiv.percona.gen.PerconaSetGenerator;



public class PerconaJOINOracle implements TestOracle<PerconaGlobalState> {
    private final PerconaGlobalState state;
    private PerconaExpressionGenerator gen;
    private PerconaSelect select;
    //private PerconaSelect DQPselect;
    private PerconaSelect TLPselect;
    private final ExpectedErrors errors = new ExpectedErrors();

    public PerconaJOINOracle(PerconaGlobalState globalState) {
        state = globalState;
        PerconaErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws Exception {
        // Randomly generate a query
        PerconaTables tables = state.getSchema().getRandomTableNonEmptyTables();
        gen = new PerconaExpressionGenerator(state).setColumns(tables.getColumns());
        List<PerconaExpression> fetchColumns = new ArrayList<>();
        fetchColumns.addAll(Randomly.nonEmptySubset(tables.getColumns()).stream()
                .map(c -> new PerconaColumnReference(c, null)).collect(Collectors.toList()));

        select = new PerconaSelect();
        select.setFetchColumns(fetchColumns);

        select.setSelectType(Randomly.fromOptions(PerconaSelect.SelectType.values()));

        boolean isDistinct = true;
        PerconaExpression ex = gen.generateExpression();
        if (Randomly.getBoolean()) {
            select.setWhereClause(ex);
        }

        // Set the join.
        List<PerconaJoin> joinExpressions = PerconaJoin.getRandomJoinClauses(tables.getTables(), state);
        select.setJoinList(joinExpressions.stream().map(j -> (PerconaExpression) j).collect(Collectors.toList()));

        // Set the from clause from the tables that are not used in the join.
        List<PerconaExpression> tableList = tables.getTables().stream().map(t -> new PerconaTableReference(t))
                .collect(Collectors.toList());
        select.setFromList(tableList);

        String originalQueryString = PerconaVisitor.asString(select);
        List<String> originalResult = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                state);

//        TLPselect = new PerconaSelect(select);

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");

        if (!firstQueryString.equals(originalQueryString)) {

            String QueryStringSJT;
            String QueryStringADT;
            String QueryStringSDT = "";

            if(select.getFromOptions() == PerconaSelect.SelectType.DISTINCT) {
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
