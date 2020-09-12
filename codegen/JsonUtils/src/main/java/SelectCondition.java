import static java.util.Objects.requireNonNull;

public class SelectCondition {
    private final Operator operator;
    private final Value left;
    private final Value right;

    public SelectCondition(Operator operator, Value left, Value right) {
        requireNonNull(operator);
        requireNonNull(left);
        requireNonNull(right);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

}
