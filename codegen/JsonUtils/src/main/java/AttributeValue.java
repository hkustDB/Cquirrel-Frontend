public class AttributeValue implements Value {

    private final String columnName;

    public AttributeValue(String columnName) {
        CheckerUtils.checkNullOrEmpty(columnName,"name");
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        return "AttributeValue{" +
                "name='" + columnName + '\'' +
                '}';
    }

    public String getColumnName() {
        return columnName;
    }
}