import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class SelectCondition extends Expression {
    private final Operator operator;
    private final Value left;
    private final Value right;

    public SelectCondition(Operator operator, Value left, Value right) {
        super(asList(left, right), operator);

        requireNonNull(operator);
        requireNonNull(left);
        requireNonNull(right);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public Operator getOperator() {
        return operator;
    }

    public Value getLeft() {
        return left;
    }

    public Value getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "SelectCondition{" +
                "operator=" + operator +
                ", left=" + left +
                ", right=" + right +
                '}';
    }
}
