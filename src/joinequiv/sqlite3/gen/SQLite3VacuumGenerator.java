package joinequiv.sqlite3.gen;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.sqlite3.SQLite3GlobalState;

/**
 * @see <a href="https://www.sqlite.org/lang_vacuum.html">VACUUM</a>
 */
public final class SQLite3VacuumGenerator {

    private SQLite3VacuumGenerator() {
    }

    public static SQLQueryAdapter executeVacuum(SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder("VACUUM");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("temp", "main"));
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot VACUUM from within a transaction",
                "cannot VACUUM - SQL statements in progress", "The database file is locked"));
    }

}
