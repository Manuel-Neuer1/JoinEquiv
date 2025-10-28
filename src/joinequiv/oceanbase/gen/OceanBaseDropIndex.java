package joinequiv.oceanbase.gen;

import joinequiv.IgnoreMeException;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.oceanbase.OceanBaseGlobalState;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseTable;

public final class OceanBaseDropIndex {

    private OceanBaseDropIndex() {
    }

    public static SQLQueryAdapter generate(OceanBaseGlobalState globalState) {
        OceanBaseTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("LOCK=NONE is not supported",
                "ALGORITHM=INPLACE is not supported", "Data truncation", "Data truncated for functional index"));
    }

}
