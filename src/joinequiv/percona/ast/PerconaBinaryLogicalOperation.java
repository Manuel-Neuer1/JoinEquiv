package joinequiv.percona.ast;

import joinequiv.Randomly;

public class PerconaBinaryLogicalOperation implements PerconaExpression {

    private final PerconaExpression left;
    private final PerconaExpression right;
    private final PerconaBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum PerconaBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                if (left.isNull() && right.isNull()) {
                    return PerconaConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return PerconaConstant.createNullConstant();
                    } else {
                        return PerconaConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return PerconaConstant.createNullConstant();
                    } else {
                        return PerconaConstant.createFalse();
                    }
                } else {
                    return PerconaConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                }
            }
        },
        OR("OR", "||") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return PerconaConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return PerconaConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return PerconaConstant.createNullConstant();
                } else {
                    return PerconaConstant.createFalse();
                }
            }
        },
        XOR("XOR") {
            @Override
            public PerconaConstant apply(PerconaConstant left, PerconaConstant right) {
                if (left.isNull() || right.isNull()) {
                    return PerconaConstant.createNullConstant();
                }
                boolean xorVal = left.asBooleanNotNull() ^ right.asBooleanNotNull();
                return PerconaConstant.createBoolean(xorVal);
            }
        };

        private final String[] textRepresentations;

        PerconaBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract PerconaConstant apply(PerconaConstant left, PerconaConstant right);

        public static PerconaBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public PerconaBinaryLogicalOperation(PerconaExpression left, PerconaExpression right, PerconaBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public PerconaExpression getLeft() {
        return left;
    }

    public PerconaBinaryLogicalOperator getOp() {
        return op;
    }

    public PerconaExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaConstant leftExpected = left.getExpectedValue();
        PerconaConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
