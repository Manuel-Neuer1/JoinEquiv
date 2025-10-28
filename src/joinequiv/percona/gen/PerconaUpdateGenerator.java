package joinequiv.percona.gen;

import joinequiv.Randomly;
import joinequiv.common.gen.AbstractUpdateGenerator;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaErrors;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.PerconaVisitor;

import java.sql.SQLException;
import java.util.List;

public class PerconaUpdateGenerator extends AbstractUpdateGenerator<PerconaColumn> {

    private final PerconaGlobalState globalState;
    private PerconaExpressionGenerator gen;

    public PerconaUpdateGenerator(PerconaGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(PerconaGlobalState globalState) throws SQLException {
        return new PerconaUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() throws SQLException {
        PerconaTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<PerconaColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new PerconaExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            PerconaErrors.addExpressionErrors(errors);
            sb.append(PerconaVisitor.asString(gen.generateExpression()));
        }
        PerconaErrors.addInsertUpdateErrors(errors);
        errors.add("doesn't have this option");

        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(PerconaColumn column) {
        if (Randomly.getBoolean()) {
            sb.append(gen.generateConstant());
        } else if (Randomly.getBoolean()) {
            sb.append("DEFAULT");
        } else {
            sb.append(PerconaVisitor.asString(gen.generateExpression()));
        }
    }

}
