package joinequiv.mysql.ast;

import joinequiv.common.ast.newast.Expression;
import joinequiv.mysql.MySQLSchema.MySQLColumn;

public interface MySQLExpression extends Expression<MySQLColumn> {

    default MySQLConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
