package joinequiv.datafusion.gen;

import java.util.List;
import java.util.stream.Collectors;

import joinequiv.IgnoreMeException;
import joinequiv.common.gen.AbstractInsertGenerator;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.datafusion.DataFusionProvider.DataFusionGlobalState;
import joinequiv.datafusion.DataFusionSchema.DataFusionColumn;
import joinequiv.datafusion.DataFusionSchema.DataFusionTable;
import joinequiv.datafusion.DataFusionToStringVisitor;

public class DataFusionInsertGenerator extends AbstractInsertGenerator<DataFusionColumn> {

    private final DataFusionGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public DataFusionInsertGenerator(DataFusionGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DataFusionGlobalState globalState, DataFusionTable targetTable) {
        return new DataFusionInsertGenerator(globalState).generate(targetTable);
    }

    private SQLQueryAdapter generate(DataFusionTable targetTable) {
        // `sb` is a global `StringBuilder` for current insert query
        sb.append("INSERT INTO ");

        if (targetTable.getColumns().isEmpty()) {
            throw new IgnoreMeException();
        }
        List<DataFusionColumn> columns = targetTable.getRandomNonEmptyColumnSubset();

        sb.append(targetTable.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns); // will finally call `insertValue()` to generate random value

        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DataFusionColumn col) {
        String val = DataFusionToStringVisitor
                .asString(new DataFusionExpressionGenerator(globalState).generateConstant(col.getType()));
        sb.append(val);
    }

}
