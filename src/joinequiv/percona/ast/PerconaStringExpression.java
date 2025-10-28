package joinequiv.percona.ast;

public class PerconaStringExpression implements PerconaExpression {

    private final String str;
    private final PerconaConstant expectedValue;

    public PerconaStringExpression(String str, PerconaConstant expectedValue) {
        this.str = str;
        this.expectedValue = expectedValue;
    }

    public String getStr() {
        return str;
    }

    @Override
    public PerconaConstant getExpectedValue() {
        return expectedValue;
    }

}
