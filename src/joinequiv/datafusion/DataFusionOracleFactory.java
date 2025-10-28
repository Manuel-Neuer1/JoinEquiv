package joinequiv.datafusion;

import java.sql.SQLException;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.NoRECOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.datafusion.gen.DataFusionExpressionGenerator;

public enum DataFusionOracleFactory implements OracleFactory<DataFusionProvider.DataFusionGlobalState> {
    NOREC {
        @Override
        public TestOracle<DataFusionProvider.DataFusionGlobalState> create(
                DataFusionProvider.DataFusionGlobalState globalState) throws SQLException {
            DataFusionExpressionGenerator gen = new DataFusionExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(DataFusionErrors.getExpectedExecutionErrors())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    QUERY_PARTITIONING_WHERE {
        @Override
        public TestOracle<DataFusionProvider.DataFusionGlobalState> create(
                DataFusionProvider.DataFusionGlobalState globalState) throws SQLException {
            DataFusionExpressionGenerator gen = new DataFusionExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                    .with(DataFusionErrors.getExpectedExecutionErrors()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    }
}
