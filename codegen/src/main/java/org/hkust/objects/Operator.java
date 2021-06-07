package org.hkust.objects;

public enum Operator {
    GREATER_THAN(">"),
    LESS_THAN("<"),
    EQUALS("=="),
    GREATER_THAN_EQUAL(">="),
    LESS_THAN_EQUAL("<="),
    SUM("+"),
    SUBTRACT("-"),
    SUMMATION("++"),
    MULTIPLY("*"),
    PRODUCT("**"),
    AVG("avg"),
    AND("&&"),
    OR("||"),
    NOT("!"),
    IF("if"),
    CASE("case"),
    COUNT("COUNT"),
    COUNT_DISTINCT("COUNT_DISTINCT");

    private String operator;

    Operator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return operator;
    }

    public static Operator getOperator(String op) {
        for (Operator operator : values()) {
            if (operator.getValue().equals(op)) {
                return operator;
            }
        }
        throw new IllegalArgumentException("Got " + op);
    }

    @Override
    public String toString() {
        return "Operator{" +
                "operator='" + operator + '\'' +
                '}';
    }
}
