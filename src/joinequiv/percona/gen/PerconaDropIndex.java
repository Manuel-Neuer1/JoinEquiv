package joinequiv.percona.gen;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;

public final class PerconaDropIndex {

    private PerconaDropIndex() {
    }

    // DROP INDEX index_name ON tbl_name
    // [algorithm_option | lock_option] ...
    //
    // algorithm_option:
    // ALGORITHM [=] {DEFAULT|INPLACE|COPY}
    //
    // lock_option:
    // LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}

    public static SQLQueryAdapter generate(PerconaGlobalState globalState) {
        PerconaTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" ALGORITHM=");
            sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" LOCK=");
            sb.append(Randomly.fromOptions("DEFAULT", "NONE", "SHARED", "EXCLUSIVE"));
        }
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("LOCK=NONE is not supported", "ALGORITHM=INPLACE is not supported",
                        "Data truncation", "Data truncated for functional index",
                        "A primary key index cannot be invisible"));
    }

}
