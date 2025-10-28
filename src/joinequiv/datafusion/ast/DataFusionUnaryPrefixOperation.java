package joinequiv.datafusion.ast;

import joinequiv.common.ast.BinaryOperatorNode;
import joinequiv.common.ast.newast.NewUnaryPrefixOperatorNode;

public class DataFusionUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionUnaryPrefixOperation(DataFusionExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}
