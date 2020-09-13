import java.util.List;

import static java.util.Objects.requireNonNull;

public class Expression {
    private final List<Value> values;
    private final Operator operator;

    public Expression(List<Value> values, Operator operator) {
        CheckerUtils.checkNullOrEmpty(values, "values");
        requireNonNull(operator);

        this.values = values;
        this.operator = operator;
    }

    public List<Value> getValues() {
        return values;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return "Expression{" +
                "values=" + values +
                ", operator=" + operator +
                '}';
    }
}
