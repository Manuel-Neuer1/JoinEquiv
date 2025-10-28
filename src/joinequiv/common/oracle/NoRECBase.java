package joinequiv.common.oracle;

import joinequiv.Main.StateLogger;
import joinequiv.MainOptions;
import joinequiv.SQLConnection;
import joinequiv.SQLGlobalState;
import joinequiv.common.query.ExpectedErrors;

public abstract class NoRECBase<S extends SQLGlobalState<?, ?>> implements TestOracle<S> {

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String optimizedQueryString;
    protected String unoptimizedQueryString;

    protected NoRECBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }

}
