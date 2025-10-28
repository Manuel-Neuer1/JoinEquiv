package joinequiv.percona.ast;

import joinequiv.IgnoreMeException;
import joinequiv.Randomly;
import joinequiv.percona.PerconaSchema.PerconaDataType;
import joinequiv.percona.ast.PerconaCastOperation.CastType;

import java.math.BigInteger;

public abstract class PerconaConstant implements PerconaExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public abstract static class PerconaNoPQSConstant extends PerconaConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public PerconaConstant isEquals(PerconaConstant rightVal) {
            return null;
        }

        @Override
        public PerconaConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public PerconaDataType getType() {
            throw throwException();
        }

        @Override
        protected PerconaConstant isLessThan(PerconaConstant rightVal) {
            throw throwException();
        }

    }

    public static class PerconaDoubleConstant extends PerconaNoPQSConstant {

        private final double val;

        public PerconaDoubleConstant(double val) {
            this.val = val;
            if (Double.isInfinite(val) || Double.isNaN(val)) {
                // seems to not be supported by Percona
                throw new IgnoreMeException();
            }
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

    }

    public static class PerconaTextConstant extends PerconaConstant {

        private final String value;
        private final boolean singleQuotes;

        public PerconaTextConstant(String value) {
            this.value = value;
            singleQuotes = Randomly.getBoolean();

        }

        private void checkIfSmallFloatingPointText() {
            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
                    && castAs(CastType.SIGNED).getInt() == 0;
            if (isSmallFloatingPointText) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            // TODO implement as cast
            for (int i = value.length(); i >= 0; i--) {
                try {
                    String substring = value.substring(0, i);
                    Double val = Double.valueOf(substring);
                    return val != 0 && !Double.isNaN(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return false;
            // return castAs(CastType.SIGNED).getInt() != 0;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

        @Override
        public PerconaConstant isEquals(PerconaConstant rightVal) {
            if (rightVal.isNull()) {
                return PerconaConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                checkIfSmallFloatingPointText();
                if (asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return castAs(CastType.SIGNED).isEquals(rightVal);
            } else if (rightVal.isString()) {
                return PerconaConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public PerconaConstant castAs(CastType type) {
            if (type == CastType.SIGNED || type == CastType.UNSIGNED) {
                String value = this.value;
                while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                    if (value.startsWith("\n")) {
                        /* workaround for https://bugs.Percona.com/bug.php?id=96294 */
                        throw new IgnoreMeException();
                    }
                    value = value.substring(1);
                }
                for (int i = value.length(); i >= 0; i--) {
                    try {
                        String substring = value.substring(0, i);
                        long val = Long.parseLong(substring);
                        return PerconaConstant.createIntConstant(val, type == CastType.SIGNED);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
                return PerconaConstant.createIntConstant(0, type == CastType.SIGNED);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public PerconaDataType getType() {
            return PerconaDataType.VARCHAR;
        }

        @Override
        protected PerconaConstant isLessThan(PerconaConstant rightVal) {
            if (rightVal.isNull()) {
                return PerconaConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                if (asBooleanNotNull()) {
                    // TODO uspport floating point
                    throw new IgnoreMeException();
                }
                checkIfSmallFloatingPointText();
                return castAs(rightVal.isSigned() ? CastType.SIGNED : CastType.UNSIGNED).isLessThan(rightVal);
            } else if (rightVal.isString()) {
                // unexpected result for '-' < "!";
                // return
                // PerconaConstant.createBoolean(value.compareToIgnoreCase(rightVal.getString()) <
                // 0);
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PerconaIntConstant extends PerconaConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;

        public PerconaIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                stringRepresentation = String.valueOf(value);
            } else {
                stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public PerconaIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            isSigned = true;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public long getInt() {
            return value;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public PerconaConstant isEquals(PerconaConstant rightVal) {
            if (rightVal.isInt()) {
                return PerconaConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((PerconaIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return PerconaConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return isEquals(rightVal.castAs(CastType.SIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PerconaConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                return new PerconaIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                return new PerconaIntConstant(value, false);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        public PerconaDataType getType() {
            return PerconaDataType.INT;
        }

        @Override
        public boolean isSigned() {
            return isSigned;
        }

        private String getStringRepr() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        protected PerconaConstant isLessThan(PerconaConstant rightVal) {
            if (rightVal.isInt()) {
                long intVal = rightVal.getInt();
                if (isSigned && rightVal.isSigned()) {
                    return PerconaConstant.createBoolean(value < intVal);
                } else {
                    return PerconaConstant.createBoolean(new BigInteger(getStringRepr())
                            .compareTo(new BigInteger(((PerconaIntConstant) rightVal).getStringRepr())) < 0);
                    // return PerconaConstant.createBoolean(Long.compareUnsigned(value, intVal) < 0);
                }
            } else if (rightVal.isNull()) {
                return PerconaConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support float
                    throw new IgnoreMeException();
                }
                return isLessThan(rightVal.castAs(isSigned ? CastType.SIGNED : CastType.UNSIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PerconaNullConstant extends PerconaConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public PerconaConstant isEquals(PerconaConstant rightVal) {
            return PerconaConstant.createNullConstant();
        }

        @Override
        public PerconaConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public PerconaDataType getType() {
            return null;
        }

        @Override
        protected PerconaConstant isLessThan(PerconaConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static PerconaConstant createNullConstant() {
        return new PerconaNullConstant();
    }

    public static PerconaConstant createIntConstant(long value) {
        return new PerconaIntConstant(value, true);
    }

    public static PerconaConstant createIntConstant(long value, boolean signed) {
        return new PerconaIntConstant(value, signed);
    }

    public static PerconaConstant createUnsignedIntConstant(long value) {
        return new PerconaIntConstant(value, false);
    }

    public static PerconaConstant createIntConstantNotAsBoolean(long value) {
        return new PerconaIntConstant(value, String.valueOf(value));
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static PerconaConstant createFalse() {
        return PerconaConstant.createIntConstant(0);
    }

    public static PerconaConstant createBoolean(boolean isTrue) {
        return PerconaConstant.createIntConstant(isTrue ? 1 : 0);
    }

    public static PerconaConstant createTrue() {
        return PerconaConstant.createIntConstant(1);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract PerconaConstant isEquals(PerconaConstant rightVal);

    public abstract PerconaConstant castAs(CastType type);

    public abstract String castAsString();

    public static PerconaConstant createStringConstant(String string) {
        return new PerconaTextConstant(string);
    }

    public abstract PerconaDataType getType();

    protected abstract PerconaConstant isLessThan(PerconaConstant rightVal);

}
