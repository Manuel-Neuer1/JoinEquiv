package joinequiv.percona.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import joinequiv.common.ast.SelectBase;
import joinequiv.common.ast.newast.Select;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaVisitor;

public class PerconaSelect extends SelectBase<PerconaExpression>
        implements PerconaExpression, Select<PerconaJoin, PerconaExpression, PerconaTable, PerconaColumn>, Cloneable {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
    private PerconaText hint;

    public enum SelectType {
        DISTINCT, ALL;
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

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return null;
    }

    public void setHint(PerconaText hint) {
        this.hint = hint;
    }

    public PerconaText getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<PerconaJoin> joinStatements) {
        List<PerconaExpression> expressions = joinStatements.stream().map(e -> (PerconaExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<PerconaJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (PerconaJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return PerconaVisitor.asString(this);
    }

    public PerconaSelect(){
        super();
    }

    // 复制构造函数
    public PerconaSelect(PerconaSelect other) {
        super();

        // 拷贝 select 类型
        this.fromOptions = other.fromOptions;

        // 拷贝 modifiers
        this.modifiers = new java.util.ArrayList<>(other.modifiers);

        // 拷贝 hint
        if (other.hint != null) {
            this.hint = other.hint; // 如果 PerconaText 可变，可使用 deepCopy: this.hint = other.hint.deepCopy();
        }

        // 拷贝 fetchColumns（必须字段）
        if (other.getFetchColumns() != null) {
            this.setFetchColumns(new java.util.ArrayList<>(other.getFetchColumns()));
        }

        // 拷贝 fromList（必须字段）
        if (other.getFromList() != null) {
            this.setFromList(new java.util.ArrayList<>(other.getFromList()));
        }

        // 拷贝 joinList
        if (other.getJoinList() != null) {
            List<PerconaExpression> copiedJoins = new java.util.ArrayList<>();
            for (PerconaExpression expr : other.getJoinList()) {
                if (expr instanceof PerconaJoin) {
                    copiedJoins.add(new PerconaJoin((PerconaJoin) expr)); // 假设 PerconaJoin 有复制构造函数
                } else {
                    copiedJoins.add(expr); // 不可变对象直接使用
                }
            }
            this.setJoinList(copiedJoins);
        }
    }

}
