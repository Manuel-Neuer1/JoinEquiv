package joinequiv.mariadb;

import java.sql.SQLException;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.NoRECOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.mariadb.gen.MariaDBExpressionGenerator;
import joinequiv.mariadb.oracle.MariaDBDQPOracle;

public enum MariaDBOracleFactory implements OracleFactory<MariaDBProvider.MariaDBGlobalState> {

    NOREC {
        @Override
        public TestOracle<MariaDBProvider.MariaDBGlobalState> create(MariaDBProvider.MariaDBGlobalState globalState)
                throws SQLException {
            MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState.getRandomly());
            ExpectedErrors errors = ExpectedErrors.newErrors().with(MariaDBErrors.getCommonErrors())
                    .with("is out of range").with("unmatched parentheses").with("nothing to repeat at offset")
                    .with("missing )").with("missing terminating ]").with("range out of order in character class")
                    .with("unrecognized character after ").with("Got error '(*VERB) not recognized or malformed")
                    .with("must be followed by").with("malformed number or name after").with("digit expected after")
                    .with("Could not create a join buffer").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    DQP {
        @Override
        public TestOracle<MariaDBProvider.MariaDBGlobalState> create(MariaDBProvider.MariaDBGlobalState globalState)
                throws SQLException {
            return new MariaDBDQPOracle(globalState);
        }
    }
}
