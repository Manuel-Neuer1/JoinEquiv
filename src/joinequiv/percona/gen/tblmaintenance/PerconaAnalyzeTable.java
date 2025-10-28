package joinequiv.percona.gen.tblmaintenance;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;

public class PerconaAnalyzeTable {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();
    private final Randomly r;

    public PerconaAnalyzeTable(List<PerconaTable> tables, Randomly r) {
        this.tables = tables;
        this.r = r;
    }

    public static SQLQueryAdapter analyze(PerconaGlobalState globalState) {
        return new PerconaAnalyzeTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty(),
                globalState.getRandomly()).generate();
    }

    private SQLQueryAdapter generate() {
        //TODO 暂时不需要 Percona 的 ANALYZE 功能
//        sb.append("ANALYZE ");
//        if (Randomly.getBoolean()) {
//            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
//        }
//        sb.append(" TABLE ");
//        if (Randomly.getBoolean()) {
//            analyzeWithoutHistogram();
//        } else {
//            if (Randomly.getBoolean()) {
//                dropHistogram();
//            } else {
//                updateHistogram();
//            }
//        }
        return new SQLQueryAdapter(sb.toString());
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private void analyzeWithoutHistogram() {
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // UPDATE HISTOGRAM ON col_name [, col_name] ...
    // [WITH N BUCKETS]
    private void updateHistogram() {
        PerconaTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" UPDATE HISTOGRAM ON ");
        List<PerconaColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(r.getInteger(1, 1024));
            sb.append(" BUCKETS");
        }
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // DROP HISTOGRAM ON col_name [, col_name] ...
    private void dropHistogram() {
        PerconaTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" DROP HISTOGRAM ON ");
        List<PerconaColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
    }

}
