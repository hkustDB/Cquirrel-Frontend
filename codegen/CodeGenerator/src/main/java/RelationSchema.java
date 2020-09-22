import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.jetbrains.annotations.Nullable;

import javax.naming.directory.AttributeInUseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RelationSchema {
    private static Map<String, Attribute> lineitemSchema;
    private static ArrayListValuedHashMap<String, Attribute> schema = new ArrayListValuedHashMap<>();

    static {
        lineitemSchema = ImmutableMap.copyOf(new HashMap<String, Attribute>() {{
            put("orderkey", new Attribute(Type.getClass("long"), 0));
            put("partkey", new Attribute(Type.getClass("long"), 1));
            put("suppkey", new Attribute(Type.getClass("long"), 2));
            put("l_linenumber", new Attribute(Type.getClass("int"), 3));
            put("l_quantity", new Attribute(Type.getClass("double"), 4));
            put("l_extendedprice", new Attribute(Type.getClass("double"), 5));
            put("l_discount", new Attribute(Type.getClass("double"), 6));
            put("l_tax", new Attribute(Type.getClass("double"), 7));
            put("l_returnflag", new Attribute(Type.getClass("char"), 8));
            put("l_linestatus", new Attribute(Type.getClass("char"), 9));
            put("shipdate", new Attribute(Type.getClass("date"), 10));
            put("l_commitdate", new Attribute(Type.getClass("date"), 11));
            put("l_receiptdate", new Attribute(Type.getClass("date"), 12));
            put("l_shipinstruct", new Attribute(Type.getClass("String"), 13));
            put("l_shipmode", new Attribute(Type.getClass("String"), 14));
            put("l_comment", new Attribute(Type.getClass("String"), 15));
        }});
        schema.putAll(lineitemSchema);
    }

    @Nullable
    public static Attribute getColumnAttribute(String columnName) {
        List<Attribute> result = schema.get(columnName);
        if (result.size() == 0) {
            return null;
        }
        if (result.size() > 1) {
            throw new RuntimeException("More than one attribute found for " + columnName + ": " + result
                    + ". Use public static Attribute getColumnAttribute(String relationName, String columnName)");
        }
        return result.get(0);
    }

    @Nullable
    public static Attribute getColumnAttribute(String relationName, String columnName) throws Exception {
        CheckerUtils.checkNullOrEmpty(relationName, "relationName");
        CheckerUtils.checkNullOrEmpty(columnName, "columnName");
        Map<String, Attribute> relationSchema = (Map<String, Attribute>) RelationSchema.class.getDeclaredField(relationName.toLowerCase() + "schema").get(new RelationSchema());
        return relationSchema.get(columnName);
    }

    public static class Attribute {
        private final Class type;
        private final int position;

        public Attribute(Class type, int position) {
            requireNonNull(type);
            if (position < 0) {
                throw new RuntimeException("position in Attribute cannot be < 0");
            }
            this.type = type;
            this.position = position;
        }
    }
}
