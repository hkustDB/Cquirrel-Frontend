import com.google.common.collect.ImmutableMap;
import com.sun.istack.internal.Nullable;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Type {
    //types with String constructor
    private static Map<String, Class> types;

    //Note: if all numbers types can't be instantiated with string use BigDecimal
    static {
        Map<String, Class> typesTemp = new HashMap<>();
        typesTemp.put("int", Integer.class);
        typesTemp.put("string", String.class);
        typesTemp.put("varchar", String.class);
        typesTemp.put("double", Double.class);
        typesTemp.put("long", Double.class);
        typesTemp.put("date", Date.class);
        typesTemp.put("char", Character.class);
        typesTemp.put("long", Long.class);
        types = ImmutableMap.copyOf(typesTemp);
    }

    @Nullable
    public static Class getClass(final String className) {
        CheckerUtils.checkNullOrEmpty(className, "className");
        String typeLower = className.toLowerCase();
        return types.get(typeLower);
    }
}
