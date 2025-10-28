package joinequiv.percona.ast;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.common.ast.BinaryOperatorNode.Operator;
import joinequiv.common.ast.UnaryOperatorNode;
import joinequiv.percona.ast.PerconaUnaryPrefixOperation.PerconaUnaryPrefixOperator;

public class PerconaUnaryPrefixOperation extends UnaryOperatorNode<PerconaExpression, PerconaUnaryPrefixOperator>
        implements PerconaExpression {

    public enum PerconaUnaryPrefixOperator implements Operator {
        NOT("!", "NOT") {
            @Override
            public PerconaConstant applyNotNull(PerconaConstant expr) {
                return PerconaConstant.createIntConstant(expr.asBooleanNotNull() ? 0 : 1);
            }
        },
        PLUS("+") {
            @Override
            public PerconaConstant applyNotNull(PerconaConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public PerconaConstant applyNotNull(PerconaConstant expr) {
                if (expr.isString()) {
                    // TODO: implement floating points
                    throw new IgnoreMeException();
                } else if (expr.isInt()) {
                    if (!expr.isSigned()) {
                        // TODO
                        throw new IgnoreMeException();
                    }
                    return PerconaConstant.createIntConstant(-expr.getInt());
                } else {
                    throw new AssertionError(expr);
                }
            }
        };

        private String[] textRepresentations;

        PerconaUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract PerconaConstant applyNotNull(PerconaConstant expr);

        public static PerconaUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public PerconaUnaryPrefixOperation(PerconaExpression expr, PerconaUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return PerconaConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
