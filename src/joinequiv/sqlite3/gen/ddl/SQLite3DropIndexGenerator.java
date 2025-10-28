package joinequiv.sqlite3.gen.ddl;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.sqlite3.SQLite3GlobalState;

// see https://www.sqlite.org/lang_dropindex.html
public final class SQLite3DropIndexGenerator {

    private SQLite3DropIndexGenerator() {
    }

    public static SQLQueryAdapter dropIndex(SQLite3GlobalState globalState) {
        String indexName = globalState.getSchema().getRandomIndexOrBailout();
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        if (Randomly.getBoolean()) {
            sb.append("IF EXISTS ");
        }
        sb.append('"');
        sb.append(indexName);
        sb.append('"');
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from(
                "[SQLITE_ERROR] SQL error or missing database (index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped)"),
                true);
    }

}
