import com.google.common.collect.ImmutableMap;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ConstantValue implements Value {
    private final Object value;
    private final Class type;
    //types with String constructor
    private static Map<String, Class> types;

    static {
        Map<String, Class> typesTemp = new HashMap<>();
        typesTemp.put("int", Integer.class);
        typesTemp.put("string", String.class);
        typesTemp.put("double", Double.class);
        typesTemp.put("long", Double.class);
        types = ImmutableMap.copyOf(typesTemp);
    }

    public Object getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    public static ConstantValue newInstance(final String val, final String type) throws Exception {
        CheckerUtils.checkNullOrEmpty(val, "val");
        CheckerUtils.checkNullOrEmpty(type, "type");

        String typeLower = type.toLowerCase();
        Object value;
        Class clss;
        if (typeLower.equals("date")) {
            DateFormat date_format = new java.text.SimpleDateFormat("yyyy-MM-dd");
            clss = Date.class;
            value = date_format.parse(val);
        } else {
            clss = types.get(typeLower);
            if (clss == null)
                throw new RuntimeException("Unknown data type: " + type);
            value = clss.getConstructor(String.class).newInstance(val);
        }

        return new ConstantValue(value, clss);
    }

    private ConstantValue(Object value, Class type) {
        this.value = value;
        this.type = type;
    }
}
