package sqlancer.mysql.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;

public class MySQLSelect extends SelectBase<MySQLExpression>
        implements MySQLExpression, Select<MySQLJoin, MySQLExpression, MySQLTable, MySQLColumn>, Cloneable {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
    private MySQLText hint;

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
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
    public MySQLConstant getExpectedValue() {
        return null;
    }

    public void setHint(MySQLText hint) {
        this.hint = hint;
    }

    public MySQLText getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<MySQLJoin> joinStatements) {
        List<MySQLExpression> expressions = joinStatements.stream().map(e -> (MySQLExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<MySQLJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (MySQLJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return MySQLVisitor.asString(this);
    }

    public MySQLSelect(){
        super();
    }

    // 复制构造函数
    public MySQLSelect(MySQLSelect other) {
        super();

        // 拷贝 select 类型
        this.fromOptions = other.fromOptions;

        // 拷贝 modifiers
        this.modifiers = new java.util.ArrayList<>(other.modifiers);

        // 拷贝 hint
        if (other.hint != null) {
            this.hint = other.hint; // 如果 MySQLText 可变，可使用 deepCopy: this.hint = other.hint.deepCopy();
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
            List<MySQLExpression> copiedJoins = new java.util.ArrayList<>();
            for (MySQLExpression expr : other.getJoinList()) {
                if (expr instanceof MySQLJoin) {
                    copiedJoins.add(new MySQLJoin((MySQLJoin) expr)); // 假设 MySQLJoin 有复制构造函数
                } else {
                    copiedJoins.add(expr); // 不可变对象直接使用
                }
            }
            this.setJoinList(copiedJoins);
        }
    }

}
