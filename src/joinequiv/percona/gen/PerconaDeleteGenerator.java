package joinequiv.percona.gen;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaErrors;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.PerconaVisitor;

import java.util.Arrays;

public class PerconaDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final PerconaGlobalState globalState;

    public PerconaDeleteGenerator(PerconaGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(PerconaGlobalState globalState) {
        return new PerconaDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        PerconaTable randomTable = globalState.getSchema().getRandomTable();
        PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        // TODO: support partitions
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(PerconaVisitor.asString(gen.generateExpression()));
            PerconaErrors.addExpressionErrors(errors);
        }
        errors.addAll(Arrays.asList("doesn't have this option",
                "Truncated incorrect DOUBLE value" /*
                                                    * ignore as a workaround for https://bugs.Percona.com/bug.php?id=95997
                                                    */, "Truncated incorrect INTEGER value",
                "Truncated incorrect DECIMAL value", "Data truncated for functional index"));
        // TODO: support ORDER BY
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
