public class Expression {
    private final Value left;
    private final Value right;
    private final Operator operator;

    public Expression(Value left, Value right, Operator operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }
}
