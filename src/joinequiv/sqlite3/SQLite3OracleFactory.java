package joinequiv.sqlite3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import joinequiv.OracleFactory;
import joinequiv.common.oracle.CompositeTestOracle;
import joinequiv.common.oracle.NoRECOracle;
import joinequiv.common.oracle.TLPWhereOracle;
import joinequiv.common.oracle.TestOracle;
import joinequiv.common.query.ExpectedErrors;
import joinequiv.sqlite3.gen.SQLite3ExpressionGenerator;
import joinequiv.sqlite3.oracle.SQLite3CODDTestOracle;
import joinequiv.sqlite3.oracle.SQLite3Fuzzer;
import joinequiv.sqlite3.oracle.SQLite3PivotedQuerySynthesisOracle;
import joinequiv.sqlite3.oracle.tlp.SQLite3TLPAggregateOracle;
import joinequiv.sqlite3.oracle.tlp.SQLite3TLPDistinctOracle;
import joinequiv.sqlite3.oracle.tlp.SQLite3TLPGroupByOracle;
import joinequiv.sqlite3.oracle.tlp.SQLite3TLPHavingOracle;

public enum SQLite3OracleFactory implements OracleFactory<SQLite3GlobalState> {
    PQS {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3PivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }

    },
    NoREC {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                    .with(SQLite3Errors.getMatchQueryErrors()).with(SQLite3Errors.getQueryErrors())
                    .with("misuse of aggregate", "misuse of window function",
                            "second argument to nth_value must be a positive integer", "no such table",
                            "no query solution", "unable to use function MATCH in the requested context")
                    .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3TLPAggregateOracle(globalState);
        }

    },
    WHERE {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                    .build();
            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3TLPDistinctOracle(globalState);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3TLPGroupByOracle(globalState);
        }
    },
    HAVING {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3TLPHavingOracle(globalState);
        }
    },
    FUZZER {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3Fuzzer(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws Exception {
            List<TestOracle<SQLite3GlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            return new CompositeTestOracle<SQLite3GlobalState>(oracles, globalState);
        }
    },
    CODDTest {
        @Override
        public TestOracle<SQLite3GlobalState> create(SQLite3GlobalState globalState) throws SQLException {
            return new SQLite3CODDTestOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    };

}
