package joinequiv.datafusion.ast;

import joinequiv.common.ast.newast.ColumnReferenceNode;
import joinequiv.datafusion.DataFusionSchema;

public class DataFusionColumnReference extends
        ColumnReferenceNode<DataFusionExpression, DataFusionSchema.DataFusionColumn> implements DataFusionExpression {
    public DataFusionColumnReference(DataFusionSchema.DataFusionColumn column) {
        super(column);
    }

}
