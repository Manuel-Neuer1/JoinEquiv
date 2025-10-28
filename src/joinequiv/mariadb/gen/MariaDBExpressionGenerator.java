package joinequiv.mariadb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinequiv.Randomly;
import joinequiv.common.gen.NoRECGenerator;
import joinequiv.common.schema.AbstractTables;
import joinequiv.mariadb.MariaDBProvider;
import joinequiv.mariadb.MariaDBSchema.MariaDBColumn;
import joinequiv.mariadb.MariaDBSchema.MariaDBDataType;
import joinequiv.mariadb.MariaDBSchema.MariaDBTable;
import joinequiv.mariadb.ast.MariaDBAggregate;
import joinequiv.mariadb.ast.MariaDBAggregate.MariaDBAggregateFunction;
import joinequiv.mariadb.ast.MariaDBBinaryOperator;
import joinequiv.mariadb.ast.MariaDBBinaryOperator.MariaDBBinaryComparisonOperator;
import joinequiv.mariadb.ast.MariaDBColumnName;
import joinequiv.mariadb.ast.MariaDBConstant;
import joinequiv.mariadb.ast.MariaDBExpression;
import joinequiv.mariadb.ast.MariaDBFunction;
import joinequiv.mariadb.ast.MariaDBFunctionName;
import joinequiv.mariadb.ast.MariaDBInOperation;
import joinequiv.mariadb.ast.MariaDBJoin;
import joinequiv.mariadb.ast.MariaDBPostfixUnaryOperation;
import joinequiv.mariadb.ast.MariaDBPostfixUnaryOperation.MariaDBPostfixUnaryOperator;
import joinequiv.mariadb.ast.MariaDBSelectStatement;
import joinequiv.mariadb.ast.MariaDBSelectStatement.MariaDBSelectType;
import joinequiv.mariadb.ast.MariaDBTableReference;
import joinequiv.mariadb.ast.MariaDBText;
import joinequiv.mariadb.ast.MariaDBUnaryPrefixOperation;
import joinequiv.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;

public class MariaDBExpressionGenerator
        implements NoRECGenerator<MariaDBSelectStatement, MariaDBJoin, MariaDBExpression, MariaDBTable, MariaDBColumn> {

    private final Randomly r;
    private List<MariaDBTable> targetTables = new ArrayList<>();
    private List<MariaDBColumn> columns = new ArrayList<>();

    public MariaDBExpressionGenerator(Randomly r) {
        this.r = r;
    }

    /* 一个静态辅助方法，负责生成一个随机的常量。它会随机选择一个数据类型（INT, VARCHAR 等），然后生成一个对应类型的随机值。 */
    public static MariaDBConstant getRandomConstant(Randomly r) {
        MariaDBDataType option = Randomly.fromOptions(MariaDBDataType.values());
        return getRandomConstant(r, option);
    }

    public static MariaDBConstant getRandomConstant(Randomly r, MariaDBDataType option) throws AssertionError {
        // 以一个很小的概率（具体来说是 1% 的概率）生成一个 SQL 的 NULL 值。
        if (Randomly.getBooleanWithSmallProbability()) {
            return MariaDBConstant.createNullConstant();
        }
        switch (option) {
        case REAL:
            // FIXME: bug workaround for MDEV-21032
            return MariaDBConstant.createIntConstant(r.getInteger());
        // double val;
        // do {
        // val = r.getDouble();
        // } while (Double.isInfinite(val));
        // return MariaDBConstant.createDoubleConstant(val);
        case INT:
            return MariaDBConstant.createIntConstant(r.getInteger());
        case VARCHAR:
            return MariaDBConstant.createTextConstant(r.getString());
        case BOOLEAN:
            return MariaDBConstant.createBooleanConstant(Randomly.getBoolean());
        default:
            throw new AssertionError(option);
        }
    }

    /* 设置列的列表 */
    public MariaDBExpressionGenerator setColumns(List<MariaDBColumn> columns) {
        this.columns = columns;
        return this;
    }

    private enum ExpressionType {
        LITERAL, COLUMN, BINARY_COMPARISON, UNARY_POSTFIX_OPERATOR, UNARY_PREFIX_OPERATOR, FUNCTION, IN
    }

    public MariaDBExpression getRandomExpression(int depth) {
        // MAX_EXPRESSION_DEPTH 默认是3
        // 递归的深度已经达到了预设的上限。这是为了保证程序能在有限时间内结束，并且生成的 SQL 不会因为过度嵌套而变得无法解析或极度缓慢
        if (depth >= MariaDBProvider.MAX_EXPRESSION_DEPTH) {
            if (Randomly.getBoolean() || columns.isEmpty()) { // 如果随机为true并且columns列表为空，返回一个随机常量
                return getRandomConstant(r);
            } else { // 否则返回一个随机列名
                return getRandomColumn();
            }
        }
        // 1. 准备可选的表达式类型
        List<ExpressionType> expressionTypes = new ArrayList<>(Arrays.asList(ExpressionType.values()));
        if (columns.isEmpty()) { // 如果没列可选，就不能生成列
            expressionTypes.remove(ExpressionType.COLUMN);
        }
        // 2. 随机选择一种类型
        ExpressionType expressionType = Randomly.fromList(expressionTypes);
        switch (expressionType) {
        case COLUMN:
            return getRandomColumn();
        case LITERAL:
            return getRandomConstant(r);
        case BINARY_COMPARISON: // 生成 [表达式1] [操作符] [表达式2]，例如 c1 > (c2 + 10)
            //它调用 getRandomExpression(depth + 1) 两次，分别生成左右两边的子表达式，然后用一个随机的比较操作符（如 =, >, <）将它们组合成一个 MariaDBBinaryOperator 对象。
            return new MariaDBBinaryOperator(getRandomExpression(depth + 1), getRandomExpression(depth + 1),
                    MariaDBBinaryComparisonOperator.getRandom());
        case UNARY_PREFIX_OPERATOR: // 生成 [操作符][表达式]，例如 NOT is_active。
            // 调用 getRandomExpression(depth + 1) 一次，生成操作数，然后组合成 MariaDBUnaryPrefixOperation 对象。
            return new MariaDBUnaryPrefixOperation(getRandomExpression(depth + 1),
                    MariaDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX_OPERATOR: // 生成 [表达式] [操作符]，例如 c1 IS TRUE。
            // 调用 getRandomExpression(depth + 1) 一次，生成操作数，然后组合成 MariaDBPostfixUnaryOperation 对象。
            return new MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator.getRandom(),
                    getRandomExpression(depth + 1));
        case FUNCTION: // 生成 FUNC([参数1], [参数2], ...)，例如 CONCAT(c1, ' - ', c2)。
            // 它会随机选择一个函数，然后根据函数所需的参数个数，多次调用 getRandomExpression(depth + 1) 来生成每个参数。
            MariaDBFunctionName func = MariaDBFunctionName.getRandom();
            return new MariaDBFunction(func, getArgs(func, depth + 1));
        case IN: // 生成 [表达式] IN ([表达式1], [表达式2], ...)。
            // 调用 getRandomExpression(depth + 1) 一次生成 IN 左边的表达式，再多次调用 getRandomExpression(depth + 1) 生成 IN (...) 列表中的表达式。
            return new MariaDBInOperation(getRandomExpression(depth + 1), getSmallNumberRandomExpressions(depth + 1),
                    Randomly.getBoolean());
        default:
            throw new AssertionError(expressionType);
        }
    }

    private List<MariaDBExpression> getSmallNumberRandomExpressions(int depth) {
        List<MariaDBExpression> expressions = new ArrayList<>(); // 创建一个空的列表，用来存放即将生成的表达式
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) { //  返回一个随机的小整数,这个 +1 非常重要，它保证了循环至少会执行一次。所以这个方法返回的列表永远不会是空的，它至少包含一个表达式。
            expressions.add(getRandomExpression(depth + 1)); // 在循环的每一次迭代中，它都递归地调用主生成方法 getRandomExpression 来创建一个新的、随机的表达式，并将其添加到列表中。同时，它正确地将递归深度 depth 加一。
        }
        return expressions; // 返回最终填充好的表达式列表
    }

    private List<MariaDBExpression> getArgs(MariaDBFunctionName func, int depth) {
        List<MariaDBExpression> expressions = new ArrayList<>();
        for (int i = 0; i < func.getNrArgs(); i++) { // func.getNrArgs() 会返回这个函数定义的固定参数数量（例如，ABS(x) 的 getNrArgs() 会返回 1）
            expressions.add(getRandomExpression(depth + 1)); // 每次都递归调用 getRandomExpression(depth + 1) 来生成一个参数
        }
        if (func.isVariadic()) { // if (func.isVariadic()): 首先检查这个函数是否支持可变参数（例如 CONCAT(s1, s2, ...)）
            for (int i = 0; i < Randomly.smallNumber(); i++) { // 如果是，它会进入第二个循环，生成**随机数量（可能为0）**的额外参数。注意这里没有 +1，意味着额外的参数是可选的
                expressions.add(getRandomExpression(depth + 1)); // 这使得它可以为 CONCAT 生成 CONCAT(c1, c2)，也可以生成 CONCAT(c1, c2, 'hello', c3+c4)
            }
        }
        return expressions;
    }

    private MariaDBExpression getRandomColumn() { // 随机选择一列并将其包装成一个表达式对象
        MariaDBColumn randomColumn = Randomly.fromList(columns); // 从这个列表中随机挑选一个 MariaDBColumn 对象
        return new MariaDBColumnName(randomColumn); // 它不直接返回代表数据库结构的 MariaDBColumn 对象，而是将其包装成一个 MariaDBColumnName 对象
    }

    public MariaDBExpression getRandomExpression() {
        return getRandomExpression(0);
    }

    @Override
    public MariaDBExpressionGenerator setTablesAndColumns(AbstractTables<MariaDBTable, MariaDBColumn> targetTables) {
        this.targetTables = targetTables.getTables();
        this.columns = targetTables.getColumns();
        return this;
    }

    @Override
    public List<MariaDBExpression> getTableRefs() {
        List<MariaDBExpression> tableRefs = new ArrayList<>();
        for (MariaDBTable t : targetTables) { //  遍历刚刚通过 setTablesAndColumns 方法设置好的目标表列表
            MariaDBTableReference tableRef = new MariaDBTableReference(t); // 对于每一张表 t，它创建一个 MariaDBTableReference 对象来包装它。这个对象在 AST 中就代表了“对表 t 的一个引用”。
            tableRefs.add(tableRef); // 将这个新创建的表引用对象添加到列表中。
        }
        return tableRefs;
    }

    @Override
    public MariaDBExpression generateBooleanExpression() {
        return getRandomExpression();
    }

    @Override
    public MariaDBSelectStatement generateSelect() {
        return new MariaDBSelectStatement();
    }

    @Override
    public List<MariaDBJoin> getRandomJoinClauses() {
        return MariaDBJoin.getRandomJoinClauses(targetTables, r);
    }

    /* 接收一个随机生成的布尔表达式（whereCondition），并将其嵌入到一个标准的、极易被数据库查询优化器优化的 SELECT 语句中 */
    @Override
    public String generateOptimizedQueryString(MariaDBSelectStatement select, MariaDBExpression whereCondition,
            boolean shouldUseAggregate) {
        if (shouldUseAggregate) {
            MariaDBAggregate aggr = new MariaDBAggregate(
                    new MariaDBColumnName(new MariaDBColumn("*", MariaDBDataType.INT, false, 0)),
                    MariaDBAggregateFunction.COUNT);
            select.setFetchColumns(Arrays.asList(aggr));
        } else {
            MariaDBColumnName aggr = new MariaDBColumnName(MariaDBColumn.createDummy("*"));
            select.setFetchColumns(Arrays.asList(aggr));
        }

        select.setWhereClause(whereCondition);
        select.setSelectType(MariaDBSelectType.ALL);
        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(MariaDBSelectStatement select, MariaDBExpression whereCondition) {
        MariaDBPostfixUnaryOperation isTrue = new MariaDBPostfixUnaryOperation(MariaDBPostfixUnaryOperator.IS_TRUE,
                whereCondition);
        MariaDBText asText = new MariaDBText(isTrue, " as count", false);
        select.setFetchColumns(Arrays.asList(asText));
        select.setSelectType(MariaDBSelectType.ALL);

        return "SELECT SUM(count) FROM (" + select.asString() + ") as asdf";
    }
}
