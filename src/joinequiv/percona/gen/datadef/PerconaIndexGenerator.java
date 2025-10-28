package joinequiv.percona.gen.datadef;

import joinequiv.Randomly;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.percona.PerconaSchema.PerconaColumn;
import joinequiv.percona.PerconaSchema.PerconaDataType;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaSchema.PerconaTable.PerconaEngine;
import joinequiv.percona.*;
import joinequiv.percona.ast.PerconaExpression;
import joinequiv.percona.gen.PerconaExpressionGenerator;

import java.util.List;

public class PerconaIndexGenerator {

    private final Randomly r;
    private StringBuilder sb = new StringBuilder();
    private boolean columnIsPrimaryKey;
    private boolean containsInPlace;
    private PerconaSchema schema;
    private final PerconaGlobalState globalState;

    public PerconaIndexGenerator(PerconaSchema schema, Randomly r, PerconaGlobalState globalState) {
        this.schema = schema;
        this.r = r;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(PerconaGlobalState globalState) {
        return new PerconaIndexGenerator(globalState.getSchema(), globalState.getRandomly(), globalState).create();
    }

    public SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();
        PerconaErrors.addExpressionErrors(errors);
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            // "FULLTEXT" TODO Column 'c3' cannot be part of FULLTEXT index
            // A SPATIAL index may only contain a geometrical type column
            sb.append("UNIQUE ");
            errors.add("Duplicate entry");
        }
        sb.append("INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        indexType();
        sb.append(" ON ");
        PerconaTable table = schema.getRandomTable();
        PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append("(");
        if (table.getEngine() == PerconaEngine.INNO_DB && Randomly.getBoolean()) {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append("(");
                PerconaExpression randExpr = gen.generateExpression();
                sb.append(PerconaVisitor.asString(randExpr));
                sb.append(")");

            }
        } else {
            List<PerconaColumn> randomColumn = table.getRandomNonEmptyColumnSubset();
            int i = 0;
            for (PerconaColumn c : randomColumn) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                if (c.isPrimaryKey()) {
                    columnIsPrimaryKey = true;
                }
                sb.append(c.getName());
                if (Randomly.getBoolean() && c.getType() == PerconaDataType.VARCHAR) {
                    sb.append("(");
                    // TODO for string
                    if (PerconaBugs.bug114534) {
                        sb.append(r.getInteger(2, 5));
                    } else {
                        sb.append(r.getInteger(1, 5));
                    }
                    sb.append(")");
                }
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("ASC", "DESC"));
                }
            }
        }
        sb.append(")");
        indexOption();
        algorithmOption();
        String string = sb.toString();
        sb = new StringBuilder();
        if (containsInPlace) {
            errors.add("ALGORITHM=INPLACE is not supported");
        }
        if (table.getEngine() == PerconaEngine.ARCHIVE) {
            errors.add("Table handler doesn't support NULL in given index.");
        }
        errors.add("A primary key index cannot be invisible");
        errors.add("Functional index on a column is not supported. Consider using a regular index instead.");
        errors.add("Incorrect usage of spatial/fulltext/hash index and explicit index order");
        errors.add("The storage engine for the table doesn't support descending indexes");
        errors.add("must include all columns");
        errors.add("cannot index the expression");
        errors.add("Data truncation: Truncated incorrect");
        errors.add("a disallowed function.");
        errors.add("Data truncation");
        errors.add("Cannot create a functional index on an expression that returns a BLOB or TEXT.");
        errors.add("used in key specification without a key length");
        errors.add("can't be used in key specification with the used table type");
        errors.add("Specified key was too long");
        errors.add("out of range");
        errors.add("Data truncated for functional index");
        errors.add("used in key specification without a key length");
        errors.add("Row size too large"); // seems to happen together with MIN_ROWS in the table declaration
        return new SQLQueryAdapter(string, errors, true);
    }

    private void algorithmOption() {
        if (Randomly.getBoolean()) {
            sb.append(" ALGORITHM");
            if (Randomly.getBoolean()) {
                sb.append("=");
            }
            sb.append(" ");
            String fromOptions = Randomly.fromOptions("DEFAULT", "INPLACE", "COPY");
            if (fromOptions.contentEquals("INPLACE")) {
                containsInPlace = true;
            }
            sb.append(fromOptions);
        }
    }

    private void indexOption() {
        if (Randomly.getBoolean()) {
            sb.append(" ");
            if (columnIsPrimaryKey) {
                // The explicit primary key cannot be made invisible.
                sb.append("VISIBLE");
            } else {
                sb.append(Randomly.fromOptions("VISIBLE", "INVISIBLE"));
            }
        }
    }

    private void indexType() {
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            sb.append(Randomly.fromOptions("BTREE", "HASH"));
        }
    }

    public void setNewSchema(PerconaSchema schema) {
        this.schema = schema;
    }
}
