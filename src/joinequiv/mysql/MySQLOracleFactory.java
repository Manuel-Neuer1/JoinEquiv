package joinequiv.mysql;

import java.sql.SQLException;
import java.util.Optional;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.CERTOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLancerResultSet;
import joinequiv.mysql.gen.MySQLExpressionGenerator;
import joinequiv.mysql.oracle.MySQLJOINOracle;

public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                    .withRegex(MySQLErrors.getExpressionRegexErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    CERT {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(MySQLErrors.getExpressionErrors())
                    .withRegex(MySQLErrors.getExpressionRegexErrors()).build();
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser = (rs) -> {
                int rowCount = rs.getInt(10);
                return Optional.of((long) rowCount);
            };
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser = (rs) -> {
                String operation = rs.getString(2);
                return Optional.of(operation);
            };

            return new CERTOracle<>(globalState, gen, expectedErrors, rowCountParser, queryPlanParser);

        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    JOIN {
        @Override
        public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
            return new MySQLJOINOracle(globalState);
        }
    };
}
