public class StringValue extends ConstantValue {
    private final String value;

    public StringValue(String value, DataType type) {
        super(type);
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}
