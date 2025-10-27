package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3TLPBase extends TernaryLogicPartitioningOracleBase<SQLite3Expression, SQLite3GlobalState>
        implements TestOracle<SQLite3GlobalState> {

    SQLite3Schema s;
    SQLite3Tables targetTables;
    SQLite3ExpressionGenerator gen;
    SQLite3Select select;

    public SQLite3TLPBase(SQLite3GlobalState state) {
        super(state);
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables(); // 从schema中随机选取一个或者多个非空表，保存在targetTables中
        gen = new SQLite3ExpressionGenerator(state).setColumns(targetTables.getColumns()); // 创建一个表达式生成器 gen
        initializeTernaryPredicateVariants();
        select = new SQLite3Select(); // SQLite 的 Select 对象
        select.setFetchColumns(generateFetchColumns()); // 调用下面的 generateFetchColumns()，来决定 SELECT 后面要查询的列（比如 SELECT * 或 SELECT c1, c2），并将结果设置到 select 对象中
        List<SQLite3Table> tables = targetTables.getTables(); // 从 targetTables 对象中再次获取表的列表，准备用于生成 FROM 和 JOIN 子句
        List<Join> joinStatements = gen.getRandomJoinClauses(tables); // 调用表达式生成器 gen 的一个方法，为选中的多个表随机生成 JOIN 子句
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s); // 将 tables 列表转换成 AST 中代表“表引用”的对象列表
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList())); // 将随机生成的 JOIN 子句设置到 select 对象中
        select.setFromList(tableRefs); // 将表引用列表设置到 select 对象的 FROM 部分
        select.setWhereClause(null);
    }

    List<SQLite3Expression> generateFetchColumns() {
        List<SQLite3Expression> columns = new ArrayList<>(); // 创建一个空列表来存放要查询的列
        if (Randomly.getBoolean()) { // 做一个 50% 的随机选择
            columns.add(new SQLite3ColumnName(SQLite3Column.createDummy("*"), null)); // 生成一个代表 * 的列名对象，并添加到列表中。最终生成的 SQL 就是 SELECT * ...
        } else {
            // 从本次查询涉及的所有列中，随机选择一个非空的子集。这意味着它可能会选择一列、多列，甚至是所有列
            // 最终生成的 SQL 可能是 SELECT c1 ... 或者 SELECT c1, c3, c5 ...
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new SQLite3ColumnName(c, null)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<SQLite3Expression> getGen() {
        return gen;
    }

}
