package joinequiv.mariadb.gen;

import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mariadb.MariaDBErrors;
import joinequiv.mariadb.MariaDBSchema;

public final class MariaDBTruncateGenerator {

    private MariaDBTruncateGenerator() {
    }

    public static SQLQueryAdapter truncate(MariaDBSchema s) {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        sb.append(s.getRandomTable().getName());
        sb.append(" ");
        MariaDBCommon.addWaitClause(sb);
        ExpectedErrors errors = new ExpectedErrors();
        MariaDBErrors.addCommonErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
