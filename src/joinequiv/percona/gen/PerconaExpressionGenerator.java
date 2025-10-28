package joinequiv.percona.gen;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.common.gen.CERTGenerator;
import joinequiv.common.gen.TLPWhereGenerator;
import joinequiv.common.gen.UntypedExpressionGenerator;
import joinequiv.common.schema.AbstractTables;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaRowValue;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.ast.PerconaAggregate.PerconaAggregateFunction;
import joinequiv.percona.ast.PerconaBinaryComparisonOperation.BinaryComparisonOperator;
import joinequiv.percona.ast.PerconaBinaryLogicalOperation.PerconaBinaryLogicalOperator;
import joinequiv.percona.ast.PerconaBinaryOperation.PerconaBinaryOperator;
import joinequiv.percona.ast.PerconaCastOperation;
import joinequiv.percona.ast.PerconaComputableFunction.PerconaFunction;
import joinequiv.percona.ast.PerconaConstant.PerconaDoubleConstant;
import joinequiv.percona.ast.PerconaOrderByTerm.PerconaOrder;
import joinequiv.percona.ast.PerconaUnaryPrefixOperation.PerconaUnaryPrefixOperator;
import joinequiv.percona.PerconaBugs;
import joinequiv.percona.PerconaGlobalState;
import joinequiv.percona.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PerconaExpressionGenerator extends UntypedExpressionGenerator<PerconaExpression, PerconaColumn>
        implements TLPWhereGenerator<PerconaSelect, PerconaJoin, PerconaExpression, PerconaTable, PerconaColumn>,
        CERTGenerator<PerconaSelect, PerconaJoin, PerconaExpression, PerconaTable, PerconaColumn> {

    private final PerconaGlobalState state;
    private PerconaRowValue rowVal;
    private List<PerconaTable> tables;

    public PerconaExpressionGenerator(PerconaGlobalState state) {
        this.state = state;
    }

    public PerconaExpressionGenerator setRowVal(PerconaRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN_OPERATOR, CASE_OPERATOR;
    }

    @Override
    public PerconaExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
        case COLUMN:
            return generateColumn();
        case LITERAL:
            return generateConstant();
        case UNARY_PREFIX_OPERATION:
            PerconaExpression subExpr = generateExpression(depth + 1);
            PerconaUnaryPrefixOperator random = PerconaUnaryPrefixOperator.getRandom();
            return new PerconaUnaryPrefixOperation(subExpr, random);
        case UNARY_POSTFIX:
            return new PerconaUnaryPostfixOperation(generateExpression(depth + 1),
                    Randomly.fromOptions(PerconaUnaryPostfixOperation.UnaryPostfixOperator.values()),
                    Randomly.getBoolean());
        case COMPUTABLE_FUNCTION:
            return getComputableFunction(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return new PerconaBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    PerconaBinaryLogicalOperator.getRandom());
        case BINARY_COMPARISON_OPERATION:
            return new PerconaBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    BinaryComparisonOperator.getRandom());
        case CAST:
            return new PerconaCastOperation(generateExpression(depth + 1), PerconaCastOperation.CastType.getRandom());
        case IN_OPERATION:
            PerconaExpression expr = generateExpression(depth + 1);
            List<PerconaExpression> rightList = new ArrayList<>();
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                rightList.add(generateExpression(depth + 1));
            }
            return new PerconaInOperation(expr, rightList, Randomly.getBoolean());
        case BINARY_OPERATION:
            if (PerconaBugs.bug99135) {
                throw new IgnoreMeException();
            }
            return new PerconaBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    PerconaBinaryOperator.getRandom());
        case EXISTS:
            return getExists();
        case BETWEEN_OPERATOR:
            if (PerconaBugs.bug99181) {
                // TODO: there are a number of bugs that are triggered by the BETWEEN operator
                throw new IgnoreMeException();
            }
            return new PerconaBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1));
        case CASE_OPERATOR:
            int nr = Randomly.smallNumber() + 1;
            return new PerconaCaseOperator(generateExpression(depth + 1), generateExpressions(nr, depth + 1),
                    generateExpressions(nr, depth + 1), generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    private PerconaExpression getExists() {
        if (Randomly.getBoolean()) {
            return new PerconaExists(new PerconaStringExpression("SELECT 1", PerconaConstant.createTrue()));
        } else {
            return new PerconaExists(new PerconaStringExpression("SELECT 1 wHERE FALSE", PerconaConstant.createFalse()));
        }
    }

    private PerconaExpression getComputableFunction(int depth) {
        PerconaFunction func = PerconaFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        PerconaExpression[] args = new PerconaExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = generateExpression(depth + 1);
        }
        return new PerconaComputableFunction(func, args);
    }

    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;

        public static ConstantType[] valuesPQS() {
            return new ConstantType[] { INT, NULL, STRING };
        }
    }

    @Override
    public PerconaExpression generateConstant() {
        ConstantType[] values;
        if (state.usesPQS()) {
            values = ConstantType.valuesPQS();
        } else {
            values = ConstantType.values();
        }
        switch (Randomly.fromOptions(values)) {
        case INT:
            return PerconaConstant.createIntConstant((int) state.getRandomly().getInteger());
        case NULL:
            return PerconaConstant.createNullConstant();
        case STRING:
            /* Replace characters that still trigger open bugs in Percona */
            String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
            return PerconaConstant.createStringConstant(string);
        case DOUBLE:
            double val = state.getRandomly().getDouble();
            return new PerconaDoubleConstant(val);
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected PerconaExpression generateColumn() {
        PerconaColumn c = Randomly.fromList(columns);
        PerconaConstant val;
        if (rowVal == null) {
            val = null;
        } else {
            val = rowVal.getValues().get(c);
        }
        return PerconaColumnReference.create(c, val);
    }

    @Override
    public PerconaExpression negatePredicate(PerconaExpression predicate) {
        return new PerconaUnaryPrefixOperation(predicate, PerconaUnaryPrefixOperator.NOT);
    }

    @Override
    public PerconaExpression isNull(PerconaExpression expr) {
        return new PerconaUnaryPostfixOperation(expr, PerconaUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<PerconaExpression> generateOrderBys() {
        List<PerconaExpression> expressions = super.generateOrderBys();
        List<PerconaExpression> newOrderBys = new ArrayList<>();
        for (PerconaExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                PerconaOrderByTerm newExpr = new PerconaOrderByTerm(expr, PerconaOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

    @Override
    public PerconaExpressionGenerator setTablesAndColumns(AbstractTables<PerconaTable, PerconaColumn> tables) {
        this.columns = tables.getColumns();
        this.tables = tables.getTables();

        return this;
    }

    @Override
    public PerconaExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public PerconaSelect generateSelect() {
        return new PerconaSelect();
    }

    @Override
    public List<PerconaJoin> getRandomJoinClauses() {
        return List.of();
    }

    @Override
    public List<PerconaExpression> getTableRefs() {
        return tables.stream().map(t -> new PerconaTableReference(t)).collect(Collectors.toList());
    }

    @Override
    public List<PerconaExpression> generateFetchColumns(boolean shouldCreateDummy) {
        return columns.stream().map(c -> new PerconaColumnReference(c, null)).collect(Collectors.toList());
    }

    @Override
    public String generateExplainQuery(PerconaSelect select) {
        return "EXPLAIN " + select.asString();
    }

    public PerconaAggregate generateAggregate() {
        PerconaAggregateFunction func = Randomly.fromOptions(PerconaAggregateFunction.values());

        if (func.isVariadic()) {
            int nrExprs = Randomly.smallNumber() + 1;
            List<PerconaExpression> exprs = IntStream.range(0, nrExprs).mapToObj(index -> generateExpression())
                    .collect(Collectors.toList());

            return new PerconaAggregate(exprs, func);
        } else {
            return new PerconaAggregate(List.of(generateExpression()), func);
        }
    }

    @Override
    public boolean mutate(PerconaSelect select) {
        List<Function<PerconaSelect, Boolean>> mutators = new ArrayList<>();

        mutators.add(this::mutateWhere);
        mutators.add(this::mutateGroupBy);
        mutators.add(this::mutateHaving);
        mutators.add(this::mutateAnd);
        mutators.add(this::mutateOr);
        mutators.add(this::mutateDistinct);

        return Randomly.fromList(mutators).apply(select);
    }

    boolean mutateDistinct(PerconaSelect select) {
        PerconaSelect.SelectType selectType = select.getFromOptions();
        if (selectType != PerconaSelect.SelectType.ALL) {
            select.setSelectType(PerconaSelect.SelectType.ALL);
            return true;
        } else {
            select.setSelectType(PerconaSelect.SelectType.DISTINCT);
            return false;
        }
    }

    boolean mutateWhere(PerconaSelect select) {
        boolean increase = select.getWhereClause() != null;
        if (increase) {
            select.setWhereClause(null);
        } else {
            select.setWhereClause(generateExpression());
        }
        return increase;
    }

    boolean mutateGroupBy(PerconaSelect select) {
        boolean increase = !select.getGroupByExpressions().isEmpty();
        if (increase) {
            select.clearGroupByExpressions();
        } else {
            select.setGroupByExpressions(select.getFetchColumns());
        }
        return increase;
    }

    boolean mutateHaving(PerconaSelect select) {
        if (select.getGroupByExpressions().isEmpty()) {
            select.setGroupByExpressions(select.getFetchColumns());
            select.setHavingClause(generateExpression());
            return false;
        } else {
            if (select.getHavingClause() == null) {
                select.setHavingClause(generateExpression());
                return false;
            } else {
                select.setHavingClause(null);
                return true;
            }
        }
    }

    boolean mutateAnd(PerconaSelect select) {
        if (select.getWhereClause() == null) {
            select.setWhereClause(generateExpression());
        } else {
            PerconaExpression newWhere = new PerconaBinaryLogicalOperation(select.getWhereClause(), generateExpression(),
                    PerconaBinaryLogicalOperator.AND);
            select.setWhereClause(newWhere);
        }
        return false;
    }

    boolean mutateOr(PerconaSelect select) {
        if (select.getWhereClause() == null) {
            select.setWhereClause(generateExpression());
            return false;
        } else {
            PerconaExpression newWhere = new PerconaBinaryLogicalOperation(select.getWhereClause(), generateExpression(),
                    PerconaBinaryLogicalOperator.OR);
            select.setWhereClause(newWhere);
            return true;
        }
    }
}
