package sqlancer.mysql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLJoin implements MySQLExpression, Join<MySQLExpression, MySQLTable, MySQLColumn> {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS;
    }

    private final MySQLTable table;
    private MySQLExpression onClause;
    private JoinType type;

    public MySQLJoin(MySQLJoin other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public MySQLJoin(MySQLTable table, MySQLExpression onClause, JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public MySQLTable getTable() {
        return table;
    }

    public MySQLExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    @Override
    public void setOnClause(MySQLExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public static List<MySQLJoin> getRandomJoinClauses(List<MySQLTable> tables, MySQLGlobalState globalState) {
        List<MySQLJoin> joinStatements = new ArrayList<>(); // 创建一个空列表，用来存放最终生成的 JOIN 子句。这是方法的返回值。
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values())); // enum JoinType : NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS
        List<MySQLColumn> columns = new ArrayList<>(); // 创建一个空列表，用来累积所有已经被加入到 JOIN 链中的表的列。这非常重要，因为 ON 条件只能使用已经“可见”的列。
        if (tables.size() > 1) { // 如果输入的 tables 列表只有一个或零个表，那么就不可能生成 JOIN 子句，所以直接跳过所有逻辑，返回一个空的 joinStatements 列表
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size()); // 随机数生成调用。它会生成一个介于 0 和 tables.size()（不含 tables.size()）之间的随机整数
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) { // 这个 for 循环会执行 nrJoinClauses 次，每次迭代都生成一个完整的 JOIN 子句
                MySQLTable table = Randomly.fromList(tables); // 随机选取一张表
                tables.remove(table); // 将选中的表从列表中移除，确保它不会在后续的 JOIN 中被重复选择
                columns.addAll(table.getColumns()); // 将刚刚选中的这张表的所有列 (table.getColumns()) 添加到 columns 列表中。这样，在为下一个 JOIN 生成 ON 条件时，就可以使用这张表以及之前所有表的列
                MySQLExpressionGenerator joinGen = new MySQLExpressionGenerator(globalState).setColumns(columns); // 创建表达式生成器

                MySQLExpression joinClause = joinGen.generateExpression(); // 创建一个一个随机的、合法的布尔表达式，这个表达式将用作 ON 子句的条件
                JoinType selectedOption = Randomly.fromList(options); // 从可用的 JoinType 列表中随机选择一个
                if (selectedOption == JoinType.NATURAL) { // 如果碰巧选中的是 NATURAL JOIN，那么它是不需要 ON 子句的
                    // NATURAL joins do not have an ON clause
                    joinClause = null; // 显式地将之前生成的 joinClause 设为 null
                }
                MySQLJoin j = new MySQLJoin(table, joinClause, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }
}
