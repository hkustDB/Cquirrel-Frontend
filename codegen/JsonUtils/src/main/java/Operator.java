public enum Operator {
    GREATER_THAN(">"),
    LESS_THAN("<"),
    EQUALS("="),
    GREATER_THAN_EQUAL(">="),
    LESS_THAN_EQUAL("=<"),
    SUM("+"),
    SUMMATION("++"),
    MULTIPLY("*"),
    PRODUCT("**"),
    AVG("~"),
    AND("&&"),
    OR("||"),
    NOT("!");

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
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        return "Operator{" +
                "operator='" + operator + '\'' +
                '}';
    }
}
