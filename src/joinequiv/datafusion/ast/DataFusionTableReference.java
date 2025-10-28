package joinequiv.datafusion.ast;

import joinequiv.common.ast.newast.TableReferenceNode;
import joinequiv.datafusion.DataFusionSchema;

public class DataFusionTableReference extends TableReferenceNode<DataFusionExpression, DataFusionSchema.DataFusionTable>
        implements DataFusionExpression {
    public DataFusionTableReference(DataFusionSchema.DataFusionTable table) {
        super(table);
    }
}
