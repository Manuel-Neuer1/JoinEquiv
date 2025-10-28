package joinequiv.percona.gen;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaErrors;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.PerconaVisitor;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class PerconaInsertGenerator {

    private final PerconaTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final PerconaGlobalState globalState;

    public PerconaInsertGenerator(PerconaGlobalState globalState, PerconaTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter insertRow(PerconaGlobalState globalState) throws SQLException {
        PerconaTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(PerconaGlobalState globalState, PerconaTable table) throws SQLException {
        if (Randomly.getBoolean()) {
            return new PerconaInsertGenerator(globalState, table).generateInsert();
        } else {
            return new PerconaInsertGenerator(globalState, table).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        if (Randomly.getBoolean()) {
            //sb.append(" ");
            //sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
        }
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            //sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<PerconaColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                sb.append(PerconaVisitor.asString(gen.generateConstant()));

            }
            sb.append(")");
        }
        PerconaErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
