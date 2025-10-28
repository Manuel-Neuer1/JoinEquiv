package joinequiv.oceanbase.ast;

import joinequiv.common.ast.newast.Expression;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseColumn;

public interface OceanBaseExpression extends Expression<OceanBaseColumn> {

    default OceanBaseConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
