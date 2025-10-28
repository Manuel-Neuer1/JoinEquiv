package joinequiv.datafusion.ast;

import joinequiv.common.ast.BinaryOperatorNode.Operator;
import joinequiv.common.ast.newast.NewBinaryOperatorNode;

public class DataFusionBinaryOperation extends NewBinaryOperatorNode<DataFusionExpression>
        implements DataFusionExpression {
    public DataFusionBinaryOperation(DataFusionExpression left, DataFusionExpression right, Operator op) {
        super(left, right, op);
    }
}
