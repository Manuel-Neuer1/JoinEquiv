package joinequiv.oceanbase.ast;

import joinequiv.Randomly;
import joinequiv.common.ast.newast.Join;
import joinequiv.oceanbase.OceanBaseGlobalState;
import joinequiv.oceanbase.OceanBaseSchema;

import joinequiv.oceanbase.OceanBaseSchema.OceanBaseColumn;
import joinequiv.oceanbase.OceanBaseSchema.OceanBaseTable;
import joinequiv.oceanbase.gen.OceanBaseExpressionGenerator;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.List;

public class OceanBaseJoin implements OceanBaseExpression, Join<OceanBaseExpression, OceanBaseTable, OceanBaseColumn> {

    @Override
    public OceanBaseConstant getExpectedValue() {
        throw new UnsupportedOperationException();
    }



    //    @Override
//    public void setOnClause(OceanBaseExpression onClause) {
//    }
    public enum JoinType {
        NATURAL, INNER, LEFT, RIGHT;
    }

    private final OceanBaseSchema.OceanBaseTable table;
    private OceanBaseExpression onClause;
    private OceanBaseJoin.JoinType type;

    public OceanBaseJoin(OceanBaseJoin other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public OceanBaseJoin(OceanBaseSchema.OceanBaseTable table, OceanBaseExpression onClause, OceanBaseJoin.JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }


    public OceanBaseTable getTable() {
        return table;
    }

    public OceanBaseExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    @Override
    public void setOnClause(OceanBaseExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(OceanBaseJoin.JoinType type) {
        this.type = type;
    }

    /**
     * 根据给定的所有表，为查询生成一系列的 JOIN 子句。
     * 这个方法假设 allSelectedTables 中的第一张表将用于 FROM 子句，
     * 并为剩余的表生成 JOIN 子句。
     *
     * @param allSelectedTables 参与查询的所有表的列表。
     * @param globalState       全局状态对象。
     * @return 一个包含 OceanBaseJoin 对象的列表。
     */
    public static List<OceanBaseJoin> getRandomJoinClauses(List<OceanBaseTable> allSelectedTables, OceanBaseGlobalState globalState) {
        List<OceanBaseJoin> joins = new ArrayList<>();
        if (allSelectedTables.size() <= 1) {
            return joins;
        }

        List<OceanBaseTable> tablesInScope = new ArrayList<>();
        tablesInScope.add(allSelectedTables.get(0));

        List<OceanBaseTable> tablesLeftToJoin = new ArrayList<>(allSelectedTables.subList(1, allSelectedTables.size()));

        while (!tablesLeftToJoin.isEmpty()) {
            OceanBaseTable tableToJoin = Randomly.fromList(tablesLeftToJoin);
            tablesLeftToJoin.remove(tableToJoin);

            List<OceanBaseColumn> availableColumns = new ArrayList<>();
            for (OceanBaseTable table : tablesInScope) {
                availableColumns.addAll(table.getColumns());
            }
            availableColumns.addAll(tableToJoin.getColumns());

            OceanBaseExpression onCondition = new OceanBaseExpressionGenerator(globalState)
                    .setColumns(availableColumns)
                    .generateExpression();

            JoinType type = Randomly.fromOptions(JoinType.INNER, JoinType.LEFT, JoinType.RIGHT);

            // ==========================================================
            // == 修正点在这里 ==
            // ==========================================================
            // 直接传递 tableToJoin 对象，而不是 new OceanBaseTableReference(tableToJoin)
            joins.add(new OceanBaseJoin(tableToJoin, onCondition, type));
            // ==========================================================

            tablesInScope.add(tableToJoin);
        }

        return joins;
    }

}
