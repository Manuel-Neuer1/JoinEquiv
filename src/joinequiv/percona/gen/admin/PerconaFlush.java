package joinequiv.percona.gen.admin;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

import java.util.List;
import java.util.stream.Collectors;

/*
 * https://dev.Percona.com/doc/refman/8.0/en/flush.html#flush-tables-variants
 */
public class PerconaFlush {

    private final List<PerconaTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public PerconaFlush(List<PerconaTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter create(PerconaGlobalState globalState) {
        return new PerconaFlush(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("FLUSH");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
            sb.append(" ");
            // TODO: | RELAY LOGS [FOR CHANNEL channel] not fully implemented
            List<String> options = Randomly.nonEmptySubset("BINARY LOGS", "ENGINE LOGS", "ERROR LOGS", "GENERAL LOGS",
                    "HOSTS", "LOGS", "PRIVILEGES", "OPTIMIZER_COSTS", "RELAY LOGS", "SLOW LOGS", "STATUS",
                    "USER_RESOURCES");
            sb.append(options.stream().collect(Collectors.joining(", ")));
        } else {
            sb.append(" ");
            sb.append("TABLES");
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
                // TODO implement READ LOCK and other variants
            }
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
