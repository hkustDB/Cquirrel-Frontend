public class DoubleValue extends ConstantValue {
    private final Double value;

    public DoubleValue(Double value, DataType type) {
        super(type);
        this.value = value;
    }

    public Double getValue() {
        return this.value;
    }
}
