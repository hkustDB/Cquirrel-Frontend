import static java.util.Objects.requireNonNull;

public class SelectCondition {
    private final Expression expression;
    //This is the operator between each select condition (AND/OR) e.g. condition1 AND condition2
    private final Operator operator;

    public SelectCondition(final Expression expression, final Operator operator) {
        requireNonNull(expression);
        requireNonNull(operator);
        if (expression.getValues().size() != 2) {
            throw new RuntimeException("There must be exactly 2 values in the expression of a select condition");
        }
        this.expression = expression;
        this.operator = operator;
    }

    public Expression getExpression() {
        return expression;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return "SelectCondition{" +
                "expression=" + expression +
                ", operator=" + operator +
                '}';
    }
}
