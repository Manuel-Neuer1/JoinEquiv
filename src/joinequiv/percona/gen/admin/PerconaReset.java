package joinequiv.percona.gen.admin;

import joinequiv.Randomly;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaGlobalState;

import java.util.stream.Collectors;

public final class PerconaReset {

    private PerconaReset() {
    }

    public static SQLQueryAdapter create(PerconaGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("RESET ");
        sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
