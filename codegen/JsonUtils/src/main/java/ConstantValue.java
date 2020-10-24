public class ConstantValue implements Value {
    private final String value;
    private final Class type;
    public String getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ConstantValue{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }

    public ConstantValue(String val, String type) {
        CheckerUtils.checkNullOrEmpty(val, "val");
        CheckerUtils.checkNullOrEmpty(type, "type");

        String typeLower = type.toLowerCase();
        Class clss = Type.getClass(typeLower);
        if (clss == null) {
            throw new RuntimeException("Unknown data type: " + type);
        }
        this.value = val;
        this.type = clss;
    }
}
