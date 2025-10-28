package joinequiv.tidb;

import java.sql.SQLException;
import java.util.Optional;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.CERTOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLancerResultSet;
import joinequiv.tidb.oracle.TiDBJOINOracle;

public enum TiDBOracleFactory implements OracleFactory<TiDBProvider.TiDBGlobalState> {
    WHERE {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    CERT {
        @Override
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(TiDBErrors.getExpressionErrors()).build();
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<Long>> rowCountParser = (rs) -> {
                String content = rs.getString(2);
                return Optional.of((long) Double.parseDouble(content));
            };
            CERTOracle.CheckedFunction<SQLancerResultSet, Optional<String>> queryPlanParser = (rs) -> {
                String operation = rs.getString(1).split("_")[0]; // Extract operation names for query plans
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
        public TestOracle<TiDBProvider.TiDBGlobalState> create(TiDBProvider.TiDBGlobalState globalState)
                throws SQLException {
            return new TiDBJOINOracle(globalState);
        }
    };

}
