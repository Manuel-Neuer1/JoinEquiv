package joinequiv.mariadb.gen;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.mariadb.MariaDBErrors;
import joinequiv.mariadb.MariaDBSchema;
import joinequiv.mariadb.MariaDBSchema.MariaDBTable;
import joinequiv.mariadb.ast.MariaDBVisitor;

public final class MariaDBInsertGenerator {

    private MariaDBInsertGenerator() {
    }

    public static SQLQueryAdapter insert(MariaDBSchema s, Randomly r) {
        MariaDBTable randomTable = s.getRandomTable();
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(randomTable.getName());
        sb.append(" VALUES (");
        for (int i = 0; i < randomTable.getColumns().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            if (Randomly.getBooleanWithSmallProbability()) {
                sb.append(MariaDBVisitor.asString(MariaDBExpressionGenerator.getRandomConstant(r)));
            } else {
                sb.append(MariaDBVisitor.asString(
                        MariaDBExpressionGenerator.getRandomConstant(r, randomTable.getColumns().get(i).getType())));
            }
        }
        sb.append(")");
        ExpectedErrors errors = new ExpectedErrors();
        MariaDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
