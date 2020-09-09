public class SelectCondition<T> {
    private final ComparisonOperator comparisonOperator;
    private final String columnName;
    private final T threshold;
    private final String type;
    private final String rightType;

    //Note: data type of threshold needs to be provided for each select condition so this class is initialized properly
    public SelectCondition(ComparisonOperator operator, String columnName, T threshold, String type, String rightType) {
        CheckerUtils.checkNullOrEmpty(columnName, "columnName");
        CheckerUtils.checkNullOrEmpty(type, "type");
        CheckerUtils.checkNullOrEmpty(rightType, "rightType");
        this.comparisonOperator = operator;
        this.columnName = columnName;
        this.threshold = threshold;
        this.type = type;
        this.rightType = rightType;
    }

    public ComparisonOperator getComparisonOperator() {
        return comparisonOperator;
    }

    public String getColumnName() {
        return columnName;
    }

    public T getThreshold() {
        return threshold;
    }

}
