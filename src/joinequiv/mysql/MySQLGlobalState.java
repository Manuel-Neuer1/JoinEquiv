
package joinequiv.mysql;

import java.sql.SQLException;

import joinequiv.SQLGlobalState;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return false;
    }

}
