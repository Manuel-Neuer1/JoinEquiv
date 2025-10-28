package joinequiv.common.oracle;

import joinequiv.GlobalState;
import joinequiv.common.gen.ExpressionGenerator;

public abstract class DocumentRemovalOracleBase<E, S extends GlobalState<?, ?, ?>> implements TestOracle<S> {

    protected E predicate;

    protected final S state;

    protected DocumentRemovalOracleBase(S state) {
        this.state = state;
    }

    protected void initializeDocumentRemovalOracle() {
        ExpressionGenerator<E> gen = getGen();
        if (gen == null) {
            throw new IllegalStateException();
        }
        predicate = gen.generatePredicate();
        if (predicate == null) {
            throw new IllegalStateException();
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

}
