package joinequiv.oceanbase.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import joinequiv.common.ast.SelectBase;
import joinequiv.common.ast.newast.Select;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseColumn;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseTable;
import joinequiv.oceanbase.OceanBaseVisitor;

public class OceanBaseSelect extends SelectBase<OceanBaseExpression>
        implements OceanBaseExpression, Select<OceanBaseJoin, OceanBaseExpression, OceanBaseTable, OceanBaseColumn> {
    // TODO 这里修改为 SelectType.DISTINCT
    // private SelectType fromOptions = SelectType.ALL;
    private SelectType fromOptions = SelectType.DISTINCT;
    private List<String> modifiers = Collections.emptyList();
    private List<OceanBaseExpression> groupBys = new ArrayList<>();
    private OceanBaseStringExpression hint;

    //TODO 这里只允许 DISTINCT
//    public enum SelectType {
//        DISTINCT, ALL;
//    }
    public enum SelectType {
        DISTINCT;
    }
    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    @Override
    public void setGroupByClause(List<OceanBaseExpression> groupBys) {
        this.groupBys = groupBys;
    }

    @Override
    public List<OceanBaseExpression> getGroupByClause() {
        return this.groupBys;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return null;
    }

    public void setHint(OceanBaseStringExpression hint) {
        this.hint = hint;
    }

    public OceanBaseStringExpression getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<OceanBaseJoin> joinStatements) {
    }

    @Override
    public List<OceanBaseJoin> getJoinClauses() {
        return List.of();
    }

    @Override
    public String asString() {
        return OceanBaseVisitor.asString(this);
    }

}
