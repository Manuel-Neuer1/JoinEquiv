package joinequiv.mysql.gen.admin;

import java.util.stream.Collectors;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mysql.MySQLGlobalState;

public final class MySQLReset {

    private MySQLReset() {
    }

    public static SQLQueryAdapter create(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("RESET ");
        sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
