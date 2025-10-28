package joinequiv.sqlite3;

import java.sql.SQLException;

import joinequiv.SQLGlobalState;
import joinequiv.sqlite3.schema.SQLite3Schema;

public class SQLite3GlobalState extends SQLGlobalState<SQLite3Options, SQLite3Schema> {

    @Override
    protected SQLite3Schema readSchema() throws SQLException {
        return SQLite3Schema.fromConnection(this);
    }

}
