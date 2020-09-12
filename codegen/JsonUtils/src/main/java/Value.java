public class Value {
    private final DataType valueType;

    public Value(DataType type) {
        valueType = type;
    }

    public DataType getType() {
        return valueType;
    }
}
