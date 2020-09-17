import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RelationSchema {
    private final String name;
    private final Map<String, Attribute> columns;

    public static RelationSchema newInstance(String name, Map<String, Attribute> columns) {
        return new RelationSchema(name, columns);
    }

    private static Map<String, Attribute> lineitemColumns;

    static {
        lineitemColumns = ImmutableMap.copyOf(new HashMap<String, Attribute>() {{
            put("l_orderkey", new Attribute(Type.getClass("long"), 0));
            put("l_partkey", new Attribute(Type.getClass("long"), 1));
            put("l_suppkey", new Attribute(Type.getClass("int"), 2));
            put("l_linenumber", new Attribute(Type.getClass("long"), 3));
            put("l_quantity", new Attribute(Type.getClass("long"), 4));
            put("l_extendedprice", new Attribute(Type.getClass("double"), 5));
            put("l_discount", new Attribute(Type.getClass("double"), 6));
            put("l_tax", new Attribute(Type.getClass("double"), 7));
            put("l_returnflag", new Attribute(Type.getClass("char"), 8));
            put("l_linestatus", new Attribute(Type.getClass("char"), 9));
            put("l_shipdate", new Attribute(Type.getClass("date"), 10));
            put("l_commitdate", new Attribute(Type.getClass("date"), 11));
            put("l_receiptdate", new Attribute(Type.getClass("date"), 12));
            put("l_shipinstruct", new Attribute(Type.getClass("char"), 13));
            put("l_shipmode", new Attribute(Type.getClass("char"), 14));
            put("l_comment", new Attribute(Type.getClass("varchar"), 15));
        }});
    }

    public static RelationSchema getLineitemSchema() {
        return newInstance("lineitem", lineitemColumns);
    }

    private RelationSchema(String name, Map<String, Attribute> columns) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        CheckerUtils.checkNullOrEmpty((Collection) columns, "columns");
        this.name = name;
        this.columns = columns;
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
