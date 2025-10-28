package joinequiv.percona.ast;

import joinequiv.Randomly;
import joinequiv.percona.PerconaSchema.PerconaDataType;
import joinequiv.percona.ast.PerconaCastOperation.CastType;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class PerconaComputableFunction implements PerconaExpression {

    private final PerconaFunction func;
    private final PerconaExpression[] args;

    public PerconaComputableFunction(PerconaFunction func, PerconaExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public PerconaFunction getFunction() {
        return func;
    }

    public PerconaExpression[] getArguments() {
        return args.clone();
    }

    public enum PerconaFunction {

        // ABS(1, "ABS") {
        // @Override
        // public PerconaConstant apply(PerconaConstant[] args, PerconaExpression[] origArgs) {
        // if (args[0].isNull()) {
        // return PerconaConstant.createNullConstant();
        // }
        // PerconaConstant intVal = args[0].castAs(CastType.SIGNED);
        // return PerconaConstant.createIntConstant(Math.abs(intVal.getInt()));
        // }
        // },
        /**
         * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/bit-functions.html#function_bit-count">Bit Functions
         *      and Operators</a>
         */
        BIT_COUNT(1, "BIT_COUNT") {

            @Override
            public PerconaConstant apply(PerconaConstant[] evaluatedArgs, PerconaExpression... args) {
                PerconaConstant arg = evaluatedArgs[0];
                if (arg.isNull()) {
                    return PerconaConstant.createNullConstant();
                } else {
                    long val = arg.castAs(CastType.SIGNED).getInt();
                    return PerconaConstant.createIntConstant(Long.bitCount(val));
                }
            }

        },
        // BENCHMARK(2, "BENCHMARK") {
        //
        // @Override
        // public PerconaConstant apply(PerconaConstant[] evaluatedArgs, PerconaExpression[] args) {
        // if (evaluatedArgs[0].isNull()) {
        // return PerconaConstant.createNullConstant();
        // }
        // if (evaluatedArgs[0].castAs(CastType.SIGNED).getInt() < 0) {
        // return PerconaConstant.createNullConstant();
        // }
        // if (Math.abs(evaluatedArgs[0].castAs(CastType.SIGNED).getInt()) > 10) {
        // throw new IgnoreMeException();
        // }
        // return PerconaConstant.createIntConstant(0);
        // }
        //
        // },
        COALESCE(2, "COALESCE") {

            @Override
            public PerconaConstant apply(PerconaConstant[] args, PerconaExpression... origArgs) {
                PerconaConstant result = PerconaConstant.createNullConstant();
                for (PerconaConstant arg : args) {
                    if (!arg.isNull()) {
                        result = PerconaConstant.createStringConstant(arg.castAsString());
                        break;
                    }
                }
                return castToMostGeneralType(result, origArgs);
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        /**
         * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/control-flow-functions.html#function_if">Flow Control
         *      Functions</a>
         */
        IF(3, "IF") {

            @Override
            public PerconaConstant apply(PerconaConstant[] args, PerconaExpression... origArgs) {
                PerconaConstant cond = args[0];
                PerconaConstant left = args[1];
                PerconaConstant right = args[2];
                PerconaConstant result;
                if (cond.isNull() || !cond.asBooleanNotNull()) {
                    result = right;
                } else {
                    result = left;
                }
                return castToMostGeneralType(result, new PerconaExpression[] { origArgs[1], origArgs[2] });

            }

        },
        /**
         * @see <a href="https://dev.Percona.com/doc/refman/8.0/en/control-flow-functions.html#function_ifnull">IFNULL</a>
         */
        IFNULL(2, "IFNULL") {

            @Override
            public PerconaConstant apply(PerconaConstant[] args, PerconaExpression... origArgs) {
                PerconaConstant result;
                if (args[0].isNull()) {
                    result = args[1];
                } else {
                    result = args[0];
                }
                return castToMostGeneralType(result, origArgs);
            }

        },
        LEAST(2, "LEAST", true) {

            @Override
            public PerconaConstant apply(PerconaConstant[] evaluatedArgs, PerconaExpression... args) {
                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }

        },
        GREATEST(2, "GREATEST", true) {
            @Override
            public PerconaConstant apply(PerconaConstant[] evaluatedArgs, PerconaExpression... args) {
                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static PerconaConstant aggregate(PerconaConstant[] evaluatedArgs, BinaryOperator<PerconaConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(arg -> arg.isNull());
            if (containsNull) {
                return PerconaConstant.createNullConstant();
            }
            PerconaConstant least = evaluatedArgs[1];
            for (PerconaConstant arg : evaluatedArgs) {
                PerconaConstant left = castToMostGeneralType(least, evaluatedArgs);
                PerconaConstant right = castToMostGeneralType(arg, evaluatedArgs);
                least = op.apply(right, left);
            }
            return castToMostGeneralType(least, evaluatedArgs);
        }

        PerconaFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        PerconaFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract PerconaConstant apply(PerconaConstant[] evaluatedArgs, PerconaExpression... args);

        public static PerconaFunction getRandomFunction() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }
    }

    @Override
    public PerconaConstant getExpectedValue() {
        PerconaConstant[] constants = new PerconaConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) {
                return null;
            }
        }
        return func.apply(constants, args);
    }

    public static PerconaConstant castToMostGeneralType(PerconaConstant cons, PerconaExpression... typeExpressions) {
        if (cons.isNull()) {
            return cons;
        }
        PerconaDataType type = getMostGeneralType(typeExpressions);
        switch (type) {
        case INT:
            if (cons.isInt()) {
                return cons;
            } else {
                return PerconaConstant.createIntConstant(cons.castAs(CastType.SIGNED).getInt());
            }
        case VARCHAR:
            return PerconaConstant.createStringConstant(cons.castAsString());
        default:
            throw new AssertionError(type);
        }
    }

    public static PerconaDataType getMostGeneralType(PerconaExpression... expressions) {
        PerconaDataType type = null;
        for (PerconaExpression expr : expressions) {
            PerconaDataType exprType;
            if (expr instanceof PerconaColumnReference) {
                exprType = ((PerconaColumnReference) expr).getColumn().getType();
            } else {
                exprType = expr.getExpectedValue().getType();
            }
            if (type == null) {
                type = exprType;
            } else if (exprType == PerconaDataType.VARCHAR) {
                type = PerconaDataType.VARCHAR;
            }

        }
        return type;
    }

}
