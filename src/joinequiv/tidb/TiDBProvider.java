package joinequiv.tidb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import joinequiv.AbstractAction;
import joinequiv.DatabaseProvider;
import joinequiv.IgnoreMeException;
import joinequiv.MainOptions;
import joinequiv.Randomly;
import joinequiv.SQLConnection;
import joinequiv.SQLGlobalState;
import joinequiv.SQLProviderAdapter;
import joinequiv.StatementExecutor;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLQueryAdapter;
import joinequiv.common.query.SQLQueryProvider;
import joinequiv.common.query.SQLancerResultSet;
import joinequiv.tidb.TiDBProvider.TiDBGlobalState;
import joinequiv.tidb.TiDBSchema.TiDBTable;
import joinequiv.tidb.gen.TiDBAlterTableGenerator;
import joinequiv.tidb.gen.TiDBAnalyzeTableGenerator;
import joinequiv.tidb.gen.TiDBDeleteGenerator;
import joinequiv.tidb.gen.TiDBDropTableGenerator;
import joinequiv.tidb.gen.TiDBIndexGenerator;
import joinequiv.tidb.gen.TiDBInsertGenerator;
import joinequiv.tidb.gen.TiDBSetGenerator;
import joinequiv.tidb.gen.TiDBTableGenerator;
import joinequiv.tidb.gen.TiDBUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        CREATE_TABLE(TiDBTableGenerator::createRandomTableStatement), // 0
        CREATE_INDEX(TiDBIndexGenerator::getQuery), // 1
        // VIEW_GENERATOR(TiDBViewGenerator::getQuery), // 2
        INSERT(TiDBInsertGenerator::getQuery), // 3
        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), // 4
        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), // 5
        UPDATE(TiDBUpdateGenerator::getQuery), // 6
        DELETE(TiDBDeleteGenerator::getQuery), // 7
        SET(TiDBSetGenerator::getQuery), // 8
        ADMIN_CHECKSUM_TABLE(
                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), // 9
        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), // 10
        DROP_TABLE(TiDBDropTableGenerator::dropTable); // 11
        // DROP_VIEW(TiDBDropViewGenerator::dropView); // 12

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {

        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case TRUNCATE:
        case DELETE:
        case ADMIN_CHECKSUM_TABLE:
            return r.getInteger(0, 2);
        case SET:
        case UPDATE:
            return r.getInteger(0, 5);
//        case VIEW_GENERATOR:
//            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
//            return r.getInteger(0, 2);
        case ALTER_TABLE:
            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        case CREATE_TABLE:
        case DROP_TABLE:
//        case DROP_VIEW:
            return 0;
        default:
            throw new AssertionError(a);
        }

    }

    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TiDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        try {
            se.executeStatements();
        } catch (SQLException e) {
            if (e.getMessage().contains(
                    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                throw new IgnoreMeException(); // TODO: drop view instead
            } else {
                throw new AssertionError(e);
            }
        }

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == TiDBOracleFactory.CERT)) {
            // Disable strict Group By constraints for ROW oracle
            globalState.executeStatement(new SQLQueryAdapter(
                    "SET @@sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"));

            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            for (TiDBTable table : globalState.getSchema().getDatabaseTables()) {
                if (!table.isView()) {
                    globalState.executeStatement(new SQLQueryAdapter("ANALYZE TABLE " + table.getName() + ";", errors));
                }
            }
        }

        // TiFlash replication settings
        if (globalState.getDbmsSpecificOptions().tiflash) {
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            for (TiDBTable table : globalState.getSchema().getDatabaseTables()) {
                if (!table.isView()) {
                    globalState.executeStatement(
                            new SQLQueryAdapter("ALTER TABLE " + table.getName() + " SET TIFLASH REPLICA 1;", errors));
                }
            }
            if (Randomly.getBoolean()) {
                globalState.executeStatement(new SQLQueryAdapter("set @@tidb_enforce_mpp=1;"));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
            if (globalState.getDbmsSpecificOptions().nonPreparePlanCache) {
                s.execute("set global tidb_enable_non_prepared_plan_cache=ON;");
            }
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(url + databaseName, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }

    @Override
    public String getQueryPlan(String selectStr, TiDBGlobalState globalState) throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        SQLQueryAdapter q = new SQLQueryAdapter("EXPLAIN FORMAT=brief " + selectStr);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("├─", "").replace("└─", "").replace("│", "").trim()
                            + ";"; // Unify format
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, TiDBGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    public boolean addRowsToAllTables(TiDBGlobalState globalState) throws Exception {
        List<TiDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (TiDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = TiDBInsertGenerator.getQuery(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
