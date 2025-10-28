package joinequiv.percona.ast;

import joinequiv.common.ast.newast.Expression;
import joinequiv.percona.PerconaSchema.PerconaColumn;

public interface PerconaExpression extends Expression<PerconaColumn> {

    default PerconaConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
