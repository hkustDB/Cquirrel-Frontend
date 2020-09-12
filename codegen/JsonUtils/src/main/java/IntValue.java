public class IntValue extends ConstantValue {
    private final int value;

    public IntValue(int value, DataType type) {
        super(type);
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }
}
