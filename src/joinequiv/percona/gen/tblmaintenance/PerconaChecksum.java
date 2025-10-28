package joinequiv.percona.gen.tblmaintenance;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/checksum-table.html">CHECKSUM TABLE Statement</a>
 */
public class PerconaChecksum {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public PerconaChecksum(List<PerconaTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter checksum(PerconaGlobalState globalState) {
        return new PerconaChecksum(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).checksum();
    }

    // CHECKSUM TABLE tbl_name [, tbl_name] ... [QUICK | EXTENDED]
    private SQLQueryAdapter checksum() {
        sb.append("CHECKSUM TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
