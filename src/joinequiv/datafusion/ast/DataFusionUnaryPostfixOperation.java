package joinequiv.datafusion.ast;

import joinequiv.common.ast.BinaryOperatorNode;
import joinequiv.common.ast.newast.NewUnaryPostfixOperatorNode;

public class DataFusionUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionUnaryPostfixOperation(DataFusionExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
}
