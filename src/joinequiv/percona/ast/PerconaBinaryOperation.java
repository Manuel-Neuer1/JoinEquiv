package joinequiv.percona.ast;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.percona.ast.PerconaCastOperation.CastType;

import java.util.function.BinaryOperator;

public class PerconaBinaryOperation implements PerconaExpression {

    private final PerconaExpression left;
    private final PerconaExpression right;
    private final PerconaBinaryOperator op;

    public enum PerconaBinaryOperator {

        AND("&") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        },
        XOR("^") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private String textRepresentation;

        private static PerconaConstant applyBitOperation(PerconaConstant left, PerconaConstant right,
                                                       BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return PerconaConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.SIGNED).getInt();
                long rightVal = right.castAs(CastType.SIGNED).getInt();
                long value = op.apply(leftVal, rightVal);
                return PerconaConstant.createUnsignedIntConstant(value);
            }
        }

        PerconaBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract PerconaConstant apply(PerconaConstant left, PerconaConstant right);

        public static PerconaBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public PerconaBinaryOperation(PerconaExpression left, PerconaExpression right, PerconaBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaConstant leftExpected = left.getExpectedValue();
        PerconaConstant rightExpected = right.getExpectedValue();

        /* workaround for https://bugs.Percona.com/bug.php?id=95960 */
        if (leftExpected.isString()) {
            String text = leftExpected.castAsString();
            while (text.startsWith(" ") || text.startsWith("\t")) {
                text = text.substring(1);
            }
            if (text.startsWith("\n") || text.startsWith(".")) {
                throw new IgnoreMeException();
            }
        }

        if (rightExpected.isString()) {
            String text = rightExpected.castAsString();
            while (text.startsWith(" ") || text.startsWith("\t")) {
                text = text.substring(1);
            }
            if (text.startsWith("\n") || text.startsWith(".")) {
                throw new IgnoreMeException();
            }
        }

        return op.apply(leftExpected, rightExpected);
    }

    public PerconaExpression getLeft() {
        return left;
    }

    public PerconaBinaryOperator getOp() {
        return op;
    }

    public PerconaExpression getRight() {
        return right;
    }

}
