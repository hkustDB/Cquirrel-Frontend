public class ConstantValue implements Value {
    private final String value;
    private final Class type;
    public String getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    public static ConstantValue newInstance(final String val, final String type) {
        CheckerUtils.checkNullOrEmpty(val, "val");
        CheckerUtils.checkNullOrEmpty(type, "type");

        String typeLower = type.toLowerCase();
        Class clss = Type.getClass(typeLower);
        if (clss == null) {
            throw new RuntimeException("Unknown data type: " + type);
        }

        return new ConstantValue(val, clss);
    }

    @Override
    public String toString() {
        return "ConstantValue{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }

    private ConstantValue(String value, Class type) {
        this.value = value;
        this.type = type;
    }
}
