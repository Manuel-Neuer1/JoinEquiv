package joinequiv.percona;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.CERTOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.common.query.SQLancerResultSet;
import joinequiv.percona.gen.PerconaExpressionGenerator;
import joinequiv.percona.oracle.PerconaJOINOracle;

import java.sql.SQLException;
import java.util.Optional;

public enum PerconaOracleFactory implements OracleFactory<PerconaGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<PerconaGlobalState> create(PerconaGlobalState globalState) throws SQLException {
            PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PerconaErrors.getExpressionErrors())
                    .withRegex(PerconaErrors.getExpressionRegexErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    CERT {
        @Override
        public TestOracle<PerconaGlobalState> create(PerconaGlobalState globalState) throws SQLException {
            PerconaExpressionGenerator gen = new PerconaExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(PerconaErrors.getExpressionErrors())
                    .withRegex(PerconaErrors.getExpressionRegexErrors()).build();
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
        public TestOracle<PerconaGlobalState> create(PerconaGlobalState globalState) throws SQLException {
            return new PerconaJOINOracle(globalState);
        }
    };
}
