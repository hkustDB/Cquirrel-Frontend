package org.hkust.codegenerator;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.objects.Type;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RelationSchema {
    private static final Map<String, Attribute> lineitemSchema;
    private static final Map<String, Attribute> OrdersSchema;
    private static final Map<String, Attribute> CustomerSchema;
    private static ArrayListValuedHashMap<String, Attribute> schema = new ArrayListValuedHashMap<>();

    static {
        lineitemSchema = ImmutableMap.copyOf(new HashMap<String, Attribute>() {{
            put("orderkey", new Attribute(Type.getClass("long"), 0, "orderkey"));
            put("partkey", new Attribute(Type.getClass("long"), 1, "partkey"));
            put("suppkey", new Attribute(Type.getClass("long"), 2, "suppkey"));
            put("l_linenumber", new Attribute(Type.getClass("int"), 3, "l_linenumber"));
            put("l_quantity", new Attribute(Type.getClass("double"), 4, "l_quantity"));
            put("l_extendedprice", new Attribute(Type.getClass("double"), 5, "l_extendedprice"));
            put("l_discount", new Attribute(Type.getClass("double"), 6, "l_discount"));
            put("l_tax", new Attribute(Type.getClass("double"), 7, "l_tax"));
            put("l_returnflag", new Attribute(Type.getClass("char"), 8, "l_returnflag"));
            put("l_linestatus", new Attribute(Type.getClass("char"), 9, "l_linestatus"));
            put("l_shipdate", new Attribute(Type.getClass("date"), 10, "l_shipdate"));
            put("l_commitdate", new Attribute(Type.getClass("date"), 11, "l_commitdate"));
            put("l_receiptdate", new Attribute(Type.getClass("date"), 12, "l_receiptdate"));
            put("l_shipinstruct", new Attribute(Type.getClass("String"), 13, "l_shipinstruct"));
            put("l_shipmode", new Attribute(Type.getClass("String"), 14, "l_shipmode"));
            put("l_comment", new Attribute(Type.getClass("String"), 15, "l_comment"));
        }});
        schema.putAll(lineitemSchema);

        OrdersSchema = ImmutableMap.copyOf(new HashMap<String, Attribute>(){{
            put("orderkey", new Attribute(Type.getClass("long"), 0, "orderkey"));
            put("o_custkey", new Attribute(Type.getClass("long"), 1, "o_custkey"));
            put("o_orderstatus", new Attribute(Type.getClass("long"), 2, "o_orderstatus"));
            put("o_totalprice", new Attribute(Type.getClass("long"), 3, "o_totalprice"));
            put("o_orderdate", new Attribute(Type.getClass("long"), 4, "o_orderdate"));
            put("o_orderpriority", new Attribute(Type.getClass("long"), 5, "o_orderpriority"));
            put("o_clerk", new Attribute(Type.getClass("long"), 6, "o_clerk"));
            put("o_shippriority", new Attribute(Type.getClass("long"), 7, "o_shippriority"));
            put("o_comment", new Attribute(Type.getClass("long"), 8, "o_comment"));
        }});
        schema.putAll(OrdersSchema);

        CustomerSchema = ImmutableMap.copyOf(new HashMap<String, Attribute>(){{
            put("custkey", new Attribute(Type.getClass("long"), 0, "custkey"));
            put("c_name", new Attribute(Type.getClass("long"), 1, "c_name"));
            put("c_address", new Attribute(Type.getClass("long"), 2, "c_address"));
            put("c_nationkey", new Attribute(Type.getClass("long"), 3, "c_nationkey"));
            put("c_phone", new Attribute(Type.getClass("long"), 4, "c_phone"));
            put("c_acctbal", new Attribute(Type.getClass("long"), 5, "c_acctbal"));
            put("c_mktsegment", new Attribute(Type.getClass("long"), 6, "c_mktsegment"));
            put("c_comment", new Attribute(Type.getClass("long"), 7, "c_comment"));
        }});
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
        private final String name;

        public Attribute(Class type, int position, String name) {
            requireNonNull(type);
            if (position < 0) {
                throw new RuntimeException("position in Attribute cannot be < 0");
            }
            this.name = name;
            this.type = type;
            this.position = position;
        }

        public Class getType() {
            return type;
        }

        public int getPosition() {
            return position;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Attribute)) return false;
            Attribute attribute = (Attribute) o;
            return position == attribute.position &&
                    Objects.equals(type, attribute.type) &&
                    Objects.equals(name, attribute.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, position, name);
        }
    }
}
