package joinequiv.percona.gen.tblmaintenance;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/optimize-table.html">OPTIMIZE TABLE Statement</a>
 */
public class PerconaOptimize {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public PerconaOptimize(List<PerconaTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter optimize(PerconaGlobalState globalState) {
        return new PerconaOptimize(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).optimize();
    }

    // OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private SQLQueryAdapter optimize() {
        sb.append("OPTIMIZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
