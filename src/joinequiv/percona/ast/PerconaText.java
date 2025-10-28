package joinequiv.percona.ast;

public class PerconaText implements PerconaExpression {

    private final String text;

    public PerconaText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
