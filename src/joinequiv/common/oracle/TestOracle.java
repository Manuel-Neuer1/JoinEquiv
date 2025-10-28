package joinequiv.common.oracle;

import joinequiv.GlobalState;
import joinequiv.Reproducer;

public interface TestOracle<G extends GlobalState<?, ?, ?>> {

    void check() throws Exception;

    default Reproducer<G> getLastReproducer() {
        return null;
    }

    default String getLastQueryString() {
        throw new AssertionError("Not supported!");
    }
}
