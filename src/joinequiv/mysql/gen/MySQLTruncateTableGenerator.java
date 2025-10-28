package joinequiv.mysql.gen;

import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mysql.MySQLGlobalState;

public final class MySQLTruncateTableGenerator {

    private MySQLTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
