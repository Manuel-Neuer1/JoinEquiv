package joinequiv.percona.gen.tblmaintenance;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;


public class PerconaCheckTable {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public PerconaCheckTable(List<PerconaTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter check(PerconaGlobalState globalState) {
        return new PerconaCheckTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).generate();
    }

    // CHECK TABLE tbl_name [, tbl_name] ... [option] ...
    //
    // option: {
    // FOR UPGRADE
    // | QUICK
    // | FAST
    // | MEDIUM
    // | EXTENDED
    // | CHANGED
    // }
    private SQLQueryAdapter generate() {
        sb.append("CHECK TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        sb.append(" ");
        List<String> options = Randomly.subset("FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED");
        sb.append(options.stream().collect(Collectors.joining(" ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
