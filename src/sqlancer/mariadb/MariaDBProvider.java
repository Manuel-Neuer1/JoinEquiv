package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableAdminCommandGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateGenerator;
import sqlancer.mariadb.gen.MariaDBUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public static final int MAX_EXPRESSION_DEPTH = 3;

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action {
        ANALYZE_TABLE, //
        CHECKSUM, //
        CHECK_TABLE, //
        CREATE_INDEX, //
        INSERT, //
        OPTIMIZE, //
        REPAIR_TABLE, //
        SET, //
        TRUNCATE, //
        UPDATE, //
    }

    /* 在一个空的 MariaDB 数据库中，以一种程序化和随机化的方式，自动创建表并执行一系列复杂的数据库操作 */
    @Override
    public void generateDatabase(MariaDBGlobalState globalState) throws Exception {
        // 获取配置选项
        MainOptions options = globalState.getOptions();

        // 当数据库中的表数量少于一个随机数（1或2）时，循环创建表   Randomly.getNotCachedInteger(1, 3) 会返回一个介于 1 和 3 之间（不包括 3）的整数，也就是 1 或 2
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.getNotCachedInteger(1, 3)) {
            // 1. 创建一个表名 (例如 "t0", "t1")
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            // 2. 使用 MariaDBTableGenerator 生成一个 "CREATE TABLE ..." 的SQL查询。
            //    这个生成器会随机决定表的列数、列名、数据类型等。 MariaDBTableGenerator.generate(...) 是一个关键的辅助类，它封装了创建随机表结构的复杂逻辑
            SQLQueryAdapter createTable = MariaDBTableGenerator.generate(tableName, globalState.getRandomly(),
                    globalState.getSchema());
            // 3. 执行这个 CREATE TABLE 查询，真正在数据库中创建这张表。  globalState.executeStatement(...) 是一个包装器，它负责将生成的 SQL 查询发送到 MariaDB 并执行
            globalState.executeStatement(createTable);
        }

        int[] nrRemaining = new int[Action.values().length]; // 数组：记录每种操作还需执行的次数
        List<Action> actions = new ArrayList<>(); // 列表：记录哪些操作被选中执行（执行次数>0）
        int total = 0; // 记录所有操作的总次数

        // 遍历 Action 枚举中的每一个可能操作
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = 0; // 本次循环中，该操作要执行的次数
            switch (action) {
                // 这些表管理操作，随机执行0或1次
            case CHECKSUM:
            case CHECK_TABLE:
            case TRUNCATE:
            case REPAIR_TABLE:
            case OPTIMIZE:
            case ANALYZE_TABLE:
            case UPDATE:
            case CREATE_INDEX:
                nrPerformed = globalState.getRandomly().getInteger(0, 2); // 随机返回 0 或 1
                break;
            case SET:
                // SET 操作固定执行20次
                nrPerformed = 20;
                break;
                // INSERT 操作的次数由配置决定，随机执行 0 到 options.getMaxNumberInserts()（默认是30） 次
            case INSERT:
                nrPerformed = globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
                break;
                // 这是一个安全检查，如果未来在 Action 枚举中增加了新类型但忘记在这里处理，程序会报错
            default:
                throw new AssertionError(action);
            }
            if (nrPerformed != 0) { // 如果要执行，就加入列表
                actions.add(action);
            }
            nrRemaining[action.ordinal()] = nrPerformed; // 在数组中记录要执行的次数
            total += nrPerformed; // 累加到总次数中
        }
        while (total != 0) {
            Action nextAction = null;
            // 1. 随机选择下一个要执行的操作
            int selection = globalState.getRandomly().getInteger(0, total);
            int previousRange = 0;
            // 这个循环实现了“轮盘赌选择”算法，执行次数越多的操作，越容易被选中
            for (int i = 0; i < nrRemaining.length; i++) {
                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                    nextAction = Action.values()[i];
                    break;
                } else {
                    previousRange += nrRemaining[i];
                }
            }
            // 2. 更新计数器
            assert nextAction != null;
            assert nrRemaining[nextAction.ordinal()] > 0;
            nrRemaining[nextAction.ordinal()]--; // 将被选中的操作的剩余次数减 1
            SQLQueryAdapter query;
            try {
                // 3. 根据选中的操作，调用相应的生成器来创建SQL查询
                switch (nextAction) {
                case CHECKSUM:
                    query = MariaDBTableAdminCommandGenerator.checksumTable(globalState.getSchema());
                    break;
                case CHECK_TABLE:
                    query = MariaDBTableAdminCommandGenerator.checkTable(globalState.getSchema());
                    break;
                case TRUNCATE:
                    query = MariaDBTruncateGenerator.truncate(globalState.getSchema());
                    break;
                case REPAIR_TABLE:
                    query = MariaDBTableAdminCommandGenerator.repairTable(globalState.getSchema());
                    break;
                case INSERT:
                    query = MariaDBInsertGenerator.insert(globalState.getSchema(), globalState.getRandomly());
                    break;
                case OPTIMIZE:
                    query = MariaDBTableAdminCommandGenerator.optimizeTable(globalState.getSchema());
                    break;
                case ANALYZE_TABLE:
                    query = MariaDBTableAdminCommandGenerator.analyzeTable(globalState.getSchema());
                    break;
                case UPDATE:
                    query = MariaDBUpdateGenerator.update(globalState.getSchema(), globalState.getRandomly());
                    break;
                case CREATE_INDEX:
                    query = MariaDBIndexGenerator.generate(globalState.getSchema());
                    break;
                case SET:
                    query = MariaDBSetGenerator.set(globalState.getRandomly(), options);
                    break;
                default:
                    throw new AssertionError(nextAction);
                }
            } catch (IgnoreMeException e) {
                // 特殊异常：如果某个生成器发现当前无法生成有效的查询
                // (例如，想在一个没有列的表中插入数据)，它会抛出这个异常。
                // 我们捕获它，只减少总数，然后继续下一次循环，相当于“跳过”了这个无效操作。
                total--;
                continue;
            }
            try {
                // 4. 执行生成的SQL查询
                globalState.executeStatement(query);
            } catch (Throwable t) {
                // 5. 如果执行过程中发生任何错误（这可能就是数据库的BUG！）
                // 打印出导致错误的SQL语句，这对于调试至关重要
                System.err.println(query.getQueryString());
                throw t;
            }
            // 6. 成功执行一个操作后，将总数减 1
            total--;
        }
    }

    public static class MariaDBGlobalState extends SQLGlobalState<MariaDBOptions, MariaDBSchema> {

        @Override
        protected MariaDBSchema readSchema() throws SQLException {
            return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public SQLConnection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        globalState.getState().logStatement("CREATE DATABASE " + globalState.getDatabaseName());
        globalState.getState().logStatement("USE " + globalState.getDatabaseName());
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MariaDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MariaDBOptions.DEFAULT_PORT;
        }
        String url = String.format("jdbc:mariadb://%s:%d", host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + globalState.getDatabaseName());
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
