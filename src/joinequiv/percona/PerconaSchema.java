package joinequiv.percona;

import joinequiv.Randomly;
import joinequiv.SQLConnection;
import joinequiv.common.schema.*;
import joinequiv.percona.PerconaSchema.PerconaTable;
import joinequiv.percona.PerconaSchema.PerconaTable.PerconaEngine;
import joinequiv.percona.ast.PerconaConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PerconaSchema extends AbstractSchema<PerconaGlobalState, PerconaTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum PerconaDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static PerconaDataType getRandom(PerconaGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(PerconaDataType.INT, PerconaDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return true;
            case VARCHAR:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

    }

    public static class PerconaColumn extends AbstractTableColumn<PerconaTable, PerconaDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public PerconaColumn(String name, PerconaDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

    }

    public static class PerconaTables extends AbstractTables<PerconaTable, PerconaColumn> {

        public PerconaTables(List<PerconaTable> tables) {
            super(tables);
        }

        public PerconaRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<PerconaColumn, PerconaConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    PerconaColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    PerconaConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = PerconaConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            value = randomRowValues.getLong(columnIndex);
                            constant = PerconaConstant.createIntConstant((long) value);
                            break;
                        case VARCHAR:
                            value = randomRowValues.getString(columnIndex);
                            constant = PerconaConstant.createStringConstant((String) value);
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new PerconaRowValue(this, values);
            }

        }

    }

    private static PerconaDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return PerconaDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
            return PerconaDataType.VARCHAR;
        case "double":
            return PerconaDataType.DOUBLE;
        case "float":
            return PerconaDataType.FLOAT;
        case "decimal":
            return PerconaDataType.DECIMAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class PerconaRowValue extends AbstractRowValue<PerconaTables, PerconaColumn, PerconaConstant> {

        PerconaRowValue(PerconaTables tables, Map<PerconaColumn, PerconaConstant> values) {
            super(tables, values);
        }

    }

    public static class PerconaTable extends AbstractRelationalTable<PerconaColumn, PerconaIndex, PerconaGlobalState> {

        public enum PerconaEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            PerconaEngine(String s) {
                this.s = s;
            }

            public static PerconaEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final PerconaEngine engine;

        public PerconaTable(String tableName, List<PerconaColumn> columns, List<PerconaIndex> indexes, PerconaEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public PerconaEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class PerconaIndex extends TableIndex {

        private PerconaIndex(String indexName) {
            super(indexName);
        }

        public static PerconaIndex create(String indexName) {
            return new PerconaIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static PerconaSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.Percona.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<PerconaTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            PerconaEngine engine = PerconaEngine.get(tableEngineStr);
                            List<PerconaColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<PerconaIndex> indexes = getIndexes(con, tableName, databaseName);
                            PerconaTable t = new PerconaTable(tableName, databaseColumns, indexes, engine);
                            for (PerconaColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new PerconaSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<PerconaIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<PerconaIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(PerconaIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<PerconaColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<PerconaColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    PerconaColumn c = new PerconaColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public PerconaSchema(List<PerconaTable> databaseTables) {
        super(databaseTables);
    }

    public PerconaTables getRandomTableNonEmptyTables() {
        return new PerconaTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
