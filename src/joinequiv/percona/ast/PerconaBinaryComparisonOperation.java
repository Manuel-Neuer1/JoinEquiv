package joinequiv.percona.ast;

import joinequiv.LikeImplementationHelper;
import joinequiv.Randomly;
import joinequiv.percona.PerconaSchema.PerconaDataType;
import joinequiv.percona.ast.PerconaUnaryPrefixOperation.PerconaUnaryPrefixOperator;

public class PerconaBinaryComparisonOperation implements PerconaExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                PerconaConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.getType() == PerconaDataType.INT) {
                    return PerconaConstant.createIntConstant(1 - isEquals.getInt());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                PerconaConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan == null) {
                    return null;
                }
                if (lessThan.getType() == PerconaDataType.INT && lessThan.getInt() == 0) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                PerconaConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == PerconaDataType.INT && equals.getInt() == 1) {
                    return PerconaConstant.createFalse();
                } else {
                    PerconaConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return PerconaConstant.createNullConstant();
                    }
                    return PerconaUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                PerconaConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == PerconaDataType.INT && equals.getInt() == 1) {
                    return PerconaConstant.createTrue();
                } else {
                    PerconaConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return PerconaConstant.createNullConstant();
                    }
                    return PerconaUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }

        },
        LIKE("LIKE") {

            @Override
            public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) {
                if (leftVal.isNull() || rightVal.isNull()) {
                    return PerconaConstant.createNullConstant();
                }
                String leftStr = leftVal.castAsString();
                String rightStr = rightVal.castAsString();
                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
                return PerconaConstant.createBoolean(matches);
            }

        };
        // https://bugs.Percona.com/bug.php?id=95908
        /*
         * IS_EQUALS_NULL_SAFE("<=>") {
         *
         * @Override public PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal) { return
         * leftVal.isEqualsNullSafe(rightVal); }
         *
         * };
         */

        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract PerconaConstant getExpectedValue(PerconaConstant leftVal, PerconaConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final PerconaExpression left;
    private final PerconaExpression right;
    private final BinaryComparisonOperator op;

    public PerconaBinaryComparisonOperation(PerconaExpression left, PerconaExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public PerconaExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public PerconaExpression getRight() {
        return right;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
