package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Expression implements Value {
    private final List<Value> values;
    private final Operator operator;

    public Expression(List<Value> values, Operator operator) {
        CheckerUtils.checkNullOrEmpty(values, "values");
        requireNonNull(operator);
        validate(values.size(), operator);
        this.values = values;
        this.operator = operator;
    }

    public List<Value> getValues() {
        return values;
    }

    public Operator getOperator() {
        return operator;
    }

    private void validate(int numOfValues, Operator operator) {
        if (numOfValues == 0) {
            throw new IllegalArgumentException("Expression must have at least 1 value");
        } else if (numOfValues == 1 && operator != Operator.NOT) {
            throw new IllegalArgumentException("Expression with 1 value can only have ! as the operator");
        } else if (numOfValues > 2) {
            if (operator != Operator.AND && operator != Operator.OR) {
                throw new IllegalArgumentException("Expression with more than 2 values can only have && or || as the operator");
            }
        }
    }

    @Override
    public String toString() {
        return "Expression{" +
                "values=" + values +
                ", operator=" + operator +
                '}';
    }
}
