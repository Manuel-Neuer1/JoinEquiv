package joinequiv.tidb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.ComparatorHelper;
import joinequiv.Randomly;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.tidb.TiDBErrors;
import joinequiv.tidb.TiDBExpressionGenerator;
import joinequiv.tidb.TiDBProvider.TiDBGlobalState;
import joinequiv.tidb.TiDBSchema;
import joinequiv.tidb.TiDBSchema.TiDBTables;
import joinequiv.tidb.ast.TiDBColumnReference;
import joinequiv.tidb.ast.TiDBExpression;
import joinequiv.tidb.ast.TiDBJoin;
import joinequiv.tidb.ast.TiDBSelect;
import joinequiv.tidb.ast.TiDBTableReference;
import joinequiv.tidb.visitor.TiDBVisitor;

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
            List<TiDBSchema.TiDBTable> fetchedTables = new ArrayList<>(tables.getTables());
            Collections.shuffle(fetchedTables);
            List<TiDBSchema.TiDBTable> twoTables = fetchedTables.subList(0, 2);
            tables = new TiDBTables(twoTables);
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
        if (Randomly.getBoolean()) {
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

//        TLPselect = new TiDBSelect(select);

        String firstQueryString = replaceJoins(originalQueryString, "NATURAL LEFT JOIN", "LEFT JOIN");
        String secondQueryString = replaceJoins(originalQueryString, "NATURAL RIGHT JOIN", "RIGHT JOIN");
        if (!firstQueryString.equals(originalQueryString)) {

            String QueryStringSJT = String.format("(%s) INTERSECT (%s)", firstQueryString, secondQueryString);
            String QueryStringADT = "(" + firstQueryString + ")" + " EXCEPT " + "(" + "(" + firstQueryString + ")"+ " EXCEPT " +"(" + secondQueryString + ")" + ")";
            String QueryStringSDT = String.format("(%s UNION %s) EXCEPT ((%s EXCEPT %s) UNION (%s EXCEPT %s))", firstQueryString, secondQueryString,firstQueryString, secondQueryString, secondQueryString, firstQueryString);
            List<String> ResultSJT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSJT, errors, state);
            List<String> ResultADT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringADT, errors, state);
            List<String> ResultSDT = ComparatorHelper.getResultSetFirstColumnAsString(QueryStringSDT, errors, state);

            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSJT, originalQueryString, List.of(QueryStringSJT), state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultADT, originalQueryString, List.of(QueryStringADT), state);
            ComparatorHelper.assumeResultSetsAreEqual(originalResult, ResultSDT, originalQueryString, List.of(QueryStringSDT), state);

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


