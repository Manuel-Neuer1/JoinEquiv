package joinequiv.percona.ast;

import joinequiv.Randomly;
import joinequiv.common.ast.newast.Join;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.gen.PerconaExpressionGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PerconaJoin implements PerconaExpression, Join<PerconaExpression, PerconaTable, PerconaColumn> {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS;
    }

    private final PerconaTable table;
    private PerconaExpression onClause;
    private JoinType type;

    public PerconaJoin(PerconaJoin other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public PerconaJoin(PerconaTable table, PerconaExpression onClause, JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public PerconaTable getTable() {
        return table;
    }

    public PerconaExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    @Override
    public void setOnClause(PerconaExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public static List<PerconaJoin> getRandomJoinClauses(List<PerconaTable> tables, PerconaGlobalState globalState) {
        List<PerconaJoin> joinStatements = new ArrayList<>(); // 创建一个空列表，用来存放最终生成的 JOIN 子句。这是方法的返回值。
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values())); // enum JoinType : NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS
        List<PerconaColumn> columns = new ArrayList<>(); // 创建一个空列表，用来累积所有已经被加入到 JOIN 链中的表的列。这非常重要，因为 ON 条件只能使用已经“可见”的列。
        if (tables.size() > 1) { // 如果输入的 tables 列表只有一个或零个表，那么就不可能生成 JOIN 子句，所以直接跳过所有逻辑，返回一个空的 joinStatements 列表
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size()); // 随机数生成调用。它会生成一个介于 0 和 tables.size()（不含 tables.size()）之间的随机整数
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) { // 这个 for 循环会执行 nrJoinClauses 次，每次迭代都生成一个完整的 JOIN 子句
                PerconaTable table = Randomly.fromList(tables); // 随机选取一张表
                tables.remove(table); // 将选中的表从列表中移除，确保它不会在后续的 JOIN 中被重复选择
                columns.addAll(table.getColumns()); // 将刚刚选中的这张表的所有列 (table.getColumns()) 添加到 columns 列表中。这样，在为下一个 JOIN 生成 ON 条件时，就可以使用这张表以及之前所有表的列
                PerconaExpressionGenerator joinGen = new PerconaExpressionGenerator(globalState).setColumns(columns); // 创建表达式生成器

                PerconaExpression joinClause = joinGen.generateExpression(); // 创建一个一个随机的、合法的布尔表达式，这个表达式将用作 ON 子句的条件
                JoinType selectedOption = Randomly.fromList(options); // 从可用的 JoinType 列表中随机选择一个
                if (selectedOption == JoinType.NATURAL) { // 如果碰巧选中的是 NATURAL JOIN，那么它是不需要 ON 子句的
                    // NATURAL joins do not have an ON clause
                    joinClause = null; // 显式地将之前生成的 joinClause 设为 null
                }
                PerconaJoin j = new PerconaJoin(table, joinClause, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }
}
