public class Expression {
    private final Value left;
    private final Value right;
    private final Operator operator;

    public Expression(Value left, Value right, Operator operator) {
        //CheckerUtils.checkNullOrEmpty(left,"left");
        //CheckerUtils.checkNullOrEmpty(right,"right");
        this.left = left;
        this.right = right;
        this.operator = operator;
    }
}
