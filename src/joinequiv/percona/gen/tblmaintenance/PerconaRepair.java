package joinequiv.percona.gen.tblmaintenance;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaSchema.PerconaTable.PerconaEngine;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/repair-table.html">REPAIR TABLE Statement</a>
 */
public class PerconaRepair {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public PerconaRepair(List<PerconaTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter repair(PerconaGlobalState globalState) {
        List<PerconaTable> tables = globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty();
        for (PerconaTable table : tables) {
            // see https://bugs.Percona.com/bug.php?id=95820
            if (table.getEngine() == PerconaEngine.MY_ISAM) {
                return new SQLQueryAdapter("SELECT 1");
            }
        }
        return new PerconaRepair(tables).repair();
    }

    // REPAIR [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    // [QUICK] [EXTENDED] [USE_FRM]
    private SQLQueryAdapter repair() {
        sb.append("REPAIR");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
