public class SelectCondition<T> {
    private final ComparisonOperator comparisonOperator;
    private final String columnName;
    private final T threshold;

    //Note: data type of threshold needs to be provided for each select condition so this class is initialized properly
    public SelectCondition(ComparisonOperator operator, String columnName, T threshold) {
        CheckerUtils.checkNullOrEmpty(columnName, "columnName");
        this.comparisonOperator = operator;
        this.columnName = columnName;
        this.threshold = threshold;
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
