import java.text.DateFormat;
import java.util.Date;

public class ConstantValue implements Value {
    private final String columnName;
    private final Object value;
    private final Class type;

    private static final DateFormat date_format = new java.text.SimpleDateFormat("yyyy-MM-dd");

    public Object getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    public static ConstantValue newInstance(final String val, final String type, final String columnName) throws Exception {
        CheckerUtils.checkNullOrEmpty(val, "val");
        CheckerUtils.checkNullOrEmpty(type, "type");
        CheckerUtils.checkNullOrEmpty(columnName, "columnName");

        String typeLower = type.toLowerCase();
        Object value;
        Class clss = Type.getClass(typeLower);
        if (clss == null) {
            throw new RuntimeException("Unknown data type: " + type);
        }

        if (clss.equals(Date.class)) {
            value = date_format.parse(val);
        } else {
            value = clss.getConstructor(String.class).newInstance(val);
        }

        return new ConstantValue(value, clss, columnName);
    }

    @Override
    public String toString() {
        return "ConstantValue{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }

    private ConstantValue(Object value, Class type, String columnName) {
        this.value = value;
        this.type = type;
        this.columnName = columnName;
    }
}
