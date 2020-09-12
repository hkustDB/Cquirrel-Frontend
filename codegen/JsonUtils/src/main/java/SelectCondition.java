public class SelectCondition {
    private final Operator operator;
    private final String columnName;
    private final String value;
    private final String type;
    private final String varType;
    private final String rightType;

    //Note: data type of threshold needs to be provided for each select condition so this class is initialized properly
    public SelectCondition(Operator operator, String columnName, String value, String type, String varType, String rightType) {
        CheckerUtils.checkNullOrEmpty(columnName, "columnName");
        CheckerUtils.checkNullOrEmpty(type, "type");
        CheckerUtils.checkNullOrEmpty(value, "threshold");
        CheckerUtils.checkNullOrEmpty(varType, "varType");
        CheckerUtils.checkNullOrEmpty(rightType, "rightType");
        this.rightType = rightType;
        this.operator = operator;
        this.columnName = columnName;
        this.value = value;
        this.type = type;
        this.varType = varType;
    }

    public Operator getOperator() {
        return operator;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getValue() {
        return value;
    }

}
