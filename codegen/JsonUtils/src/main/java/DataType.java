public enum DataType {
    DOUBLE("Double"),
    INT("Int"),
    VARCHAR("String"),
    DATE("Date"),
    Long("=<");

    private String type;

    DataType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }
}
