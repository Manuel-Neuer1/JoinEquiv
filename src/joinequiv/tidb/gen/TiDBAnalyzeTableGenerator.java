package joinequiv.tidb.gen;

import java.sql.SQLException;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.tidb.TiDBErrors;
import joinequiv.tidb.TiDBProvider.TiDBGlobalState;
import joinequiv.tidb.TiDBSchema.TiDBTable;

public final class TiDBAnalyzeTableGenerator {

    private TiDBAnalyzeTableGenerator() {
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        boolean analyzeIndex = !table.getIndexes().isEmpty() && Randomly.getBoolean();
        StringBuilder sb = new StringBuilder("ANALYZE TABLE ");
        sb.append(table.getName());
        if (analyzeIndex) {
            sb.append(" INDEX ");
            sb.append(table.getRandomIndex().getIndexName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(Randomly.getNotCachedInteger(1, 1024));
            sb.append(" BUCKETS");
        }
        errors.add("Fast analyze hasn't reached General Availability and only support analyze version 1 currently");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
