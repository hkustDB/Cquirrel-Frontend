package org.hkust.schema;

import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.hkust.objects.Type;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class RelationSchema {
    public static final Schema lineitem;
    public static final Schema orders;
    public static final Schema customer;
    private static ArrayListValuedHashMap<String, Attribute> schema = new ArrayListValuedHashMap<>();

    static {
        Attribute lineitemPrimaryKey1 = new Attribute(Type.getClass("int"), 3, "l_linenumber");
        Attribute lineitemPrimaryKey2 = new Attribute(Type.getClass("long"), 0, "orderkey");
        lineitem = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", lineitemPrimaryKey2);
                    put("partkey", new Attribute(Type.getClass("long"), 1, "partkey"));
                    put("suppkey", new Attribute(Type.getClass("long"), 2, "suppkey"));
                    put("l_linenumber", lineitemPrimaryKey1);
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
                }})
                .withPrimaryKey(Arrays.asList(lineitemPrimaryKey1, lineitemPrimaryKey2))
                .withParent(null)
                .build();

        Attribute ordersPrimaryKey = new Attribute(Type.getClass("long"), 0, "orderkey");
        orders = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", ordersPrimaryKey);
                    put("custkey", new Attribute(Type.getClass("long"), 1, "o_custkey"));
                    put("o_orderstatus", new Attribute(Type.getClass("long"), 2, "o_orderstatus"));
                    put("o_totalprice", new Attribute(Type.getClass("long"), 3, "o_totalprice"));
                    put("o_orderdate", new Attribute(Type.getClass("long"), 4, "o_orderdate"));
                    put("o_orderpriority", new Attribute(Type.getClass("long"), 5, "o_orderpriority"));
                    put("o_clerk", new Attribute(Type.getClass("long"), 6, "o_clerk"));
                    put("o_shippriority", new Attribute(Type.getClass("long"), 7, "o_shippriority"));
                    put("o_comment", new Attribute(Type.getClass("long"), 8, "o_comment"));
                }})
                .withPrimaryKey(Collections.singletonList(ordersPrimaryKey))
                .withParent(lineitem)
                .build();

        Attribute customerPrimaryKey = new Attribute(Type.getClass("long"), 0, "custkey");
        customer = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("custkey", customerPrimaryKey);
                    put("c_name", new Attribute(Type.getClass("long"), 1, "c_name"));
                    put("c_address", new Attribute(Type.getClass("long"), 2, "c_address"));
                    put("c_nationkey", new Attribute(Type.getClass("long"), 3, "c_nationkey"));
                    put("c_phone", new Attribute(Type.getClass("long"), 4, "c_phone"));
                    put("c_acctbal", new Attribute(Type.getClass("long"), 5, "c_acctbal"));
                    put("c_mktsegment", new Attribute(Type.getClass("long"), 6, "c_mktsegment"));
                    put("c_comment", new Attribute(Type.getClass("long"), 7, "c_comment"));
                }})
                .withParent(orders)
                .withPrimaryKey(Collections.singletonList(customerPrimaryKey))
                .build();

    }

    /*@Nullable
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
    }*/

    @Nullable
    public static Attribute getColumnAttribute(Relation relation, String columnName) {
        switch (relation) {
            case LINEITEM:
                return lineitem.getAttributes().get(columnName);
            case ORDERS:
                return orders.getAttributes().get(columnName);
            case CUSTOMER:
                return customer.getAttributes().get(columnName);
            default:
                throw new RuntimeException("Unknown relation name");
        }
    }


}
