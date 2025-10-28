
package joinequiv.percona;

import joinequiv.SQLGlobalState;

import java.sql.SQLException;

public class PerconaGlobalState extends SQLGlobalState<PerconaOptions, PerconaSchema> {

    @Override
    protected PerconaSchema readSchema() throws SQLException {
        return PerconaSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return false;
    }

}
