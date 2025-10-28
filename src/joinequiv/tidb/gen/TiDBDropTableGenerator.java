package joinequiv.tidb.gen;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.tidb.TiDBProvider.TiDBGlobalState;

public final class TiDBDropTableGenerator {

    private TiDBDropTableGenerator() {
    }

    public static SQLQueryAdapter dropTable(TiDBGlobalState globalState) {
        if (globalState.getSchema().getTables(t -> !t.isView()).size() <= 1) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF EXISTS ");
        }
        sb.append(globalState.getSchema().getRandomTableOrBailout(t -> !t.isView()).getName());
        return new SQLQueryAdapter(sb.toString(), null, true);
    }

}
