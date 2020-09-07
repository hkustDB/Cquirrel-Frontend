public enum ComparisonOperator {
    GREATER_THAN(">"),
    LESS_THAN("<"),
    EQUALS("="),
    GREATER_THAN_EQUAL(">="),
    LESS_THAN_EQUAL("=<");

    private String operator;

    ComparisonOperator(String type) {
        this.operator = type;
    }

    public String type() {
        return operator;
    }
}
