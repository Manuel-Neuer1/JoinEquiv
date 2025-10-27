package sqlancer.tidb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBSelect extends SelectBase<TiDBExpression>
        implements TiDBExpression, Select<TiDBJoin, TiDBExpression, TiDBTable, TiDBColumn>, Cloneable {

    private TiDBExpression hint;

    public void setHint(TiDBExpression hint) {
        this.hint = hint;
    }

    public TiDBExpression getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<TiDBJoin> joinStatements) {
        List<TiDBExpression> expressions = joinStatements.stream().map(e -> (TiDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<TiDBJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (TiDBJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return TiDBVisitor.asString(this);
    }


    public TiDBSelect(){
        super();
    }

    // 复制构造函数
    public TiDBSelect(TiDBSelect other) {
        super();

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
            List<TiDBExpression> copiedJoins = new java.util.ArrayList<>();
            for (TiDBExpression expr : other.getJoinList()) {
                if (expr instanceof TiDBJoin) {
                    copiedJoins.add(new TiDBJoin((TiDBJoin) expr)); // 假设 MySQLJoin 有复制构造函数
                } else {
                    copiedJoins.add(expr); // 不可变对象直接使用
                }
            }
            this.setJoinList(copiedJoins);
        }
    }
}
