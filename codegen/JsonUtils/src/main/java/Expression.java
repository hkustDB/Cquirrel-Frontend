public class Expression {
    private final String left;
    private final String right;
    private final Operator operator;

    public Expression(String left, String right, Operator operator) {
        CheckerUtils.checkNullOrEmpty(left,"left");
        CheckerUtils.checkNullOrEmpty(right,"right");
        this.left = left;
        this.right = right;
        this.operator = operator;
    }
}
