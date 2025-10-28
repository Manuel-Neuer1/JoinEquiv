package joinequiv.percona.gen;

import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaGlobalState;

public final class PerconaTruncateTableGenerator {

    private PerconaTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(PerconaGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
