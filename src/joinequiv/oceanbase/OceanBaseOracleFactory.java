package joinequiv.oceanbase;

import java.sql.SQLException;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.NoRECOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;

import joinequiv.oceanbase.gen.OceanBaseExpressionGenerator;
import joinequiv.oceanbase.oracle.OceanBasePivotedQuerySynthesisOracle;
import joinequiv.oceanbase.oracle.OceanBaseDQPOracle;

public enum OceanBaseOracleFactory implements OracleFactory<OceanBaseGlobalState> {

    TLP_WHERE {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                    .withRegex(OceanBaseErrors.getExpressionErrorsRegex()).with("value is out of range").build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    NoREC {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                    .withRegex(OceanBaseErrors.getExpressionErrorsRegex())
                    .with("canceling statement due to statement timeout").with("unmatched parentheses")
                    .with("nothing to repeat at offset").with("missing )").with("missing terminating ]")
                    .with("range out of order in character class").with("unrecognized character after ")
                    .with("Got error '(*VERB) not recognized or malformed").with("must be followed by")
                    .with("malformed number or name after").with("digit expected after").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    PQS {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            return new OceanBasePivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    DQP {
        @Override
        public TestOracle<OceanBaseGlobalState> create(OceanBaseGlobalState globalState) throws SQLException {
            return new OceanBaseDQPOracle(globalState);
        }
    }
}
