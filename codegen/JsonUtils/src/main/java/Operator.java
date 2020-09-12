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
    AVG("~");

    private String operator;

    Operator(String type) {
        this.operator = type;
    }

    public String type() {
        return operator;
    }
}
