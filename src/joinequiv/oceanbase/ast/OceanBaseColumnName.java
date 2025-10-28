package joinequiv.oceanbase.ast;

import joinequiv.oceanbase.OceanBaseSchema;

public class OceanBaseColumnName implements OceanBaseExpression {

    private final OceanBaseSchema.OceanBaseColumn column;

    public OceanBaseColumnName(OceanBaseSchema.OceanBaseColumn column) {
        this.column = column;
    }

    public OceanBaseSchema.OceanBaseColumn getColumn() {
        return column;
    }

}
