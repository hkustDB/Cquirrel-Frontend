package org.hkust.schema;

import com.google.common.collect.ImmutableMap;
import org.hkust.objects.Type;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class RelationSchema {
    public final Schema lineitem;
    public final Schema orders;
    public final Schema customer;

    Map<Relation, Schema> SCHEMAS;

    public RelationSchema() {
        Attribute lineitemPrimaryKey1 = new Attribute(Type.getClass("int"), 3, "linenumber");
        Attribute lineitemPrimaryKey2 = new Attribute(Type.getClass("long"), 0, "orderkey");
        lineitem = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", lineitemPrimaryKey2);
                    put("partkey", new Attribute(Type.getClass("long"), 1, "partkey"));
                    put("suppkey", new Attribute(Type.getClass("long"), 2, "suppkey"));
                    put("l_linenumber", lineitemPrimaryKey1);
                    put("l_quantity", new Attribute(Type.getClass("double"), 4, "quantity"));
                    put("l_extendedprice", new Attribute(Type.getClass("double"), 5, "extendedprice"));
                    put("l_discount", new Attribute(Type.getClass("double"), 6, "discount"));
                    put("l_tax", new Attribute(Type.getClass("double"), 7, "tax"));
                    put("l_returnflag", new Attribute(Type.getClass("char"), 8, "returnflag"));
                    put("l_linestatus", new Attribute(Type.getClass("char"), 9, "linestatus"));
                    put("l_shipdate", new Attribute(Type.getClass("date"), 10, "shipdate"));
                    put("l_commitdate", new Attribute(Type.getClass("date"), 11, "commitdate"));
                    put("l_receiptdate", new Attribute(Type.getClass("date"), 12, "receiptdate"));
                    put("l_shipinstruct", new Attribute(Type.getClass("String"), 13, "shipinstruct"));
                    put("l_shipmode", new Attribute(Type.getClass("String"), 14, "shipmode"));
                    put("l_comment", new Attribute(Type.getClass("String"), 15, "l_comment"));
                }})
                .withPrimaryKey(Arrays.asList(lineitemPrimaryKey1, lineitemPrimaryKey2))
                .withParent(null)
                .build();

        Attribute ordersPrimaryKey = new Attribute(Type.getClass("long"), 0, "orderkey");
        orders = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", ordersPrimaryKey);
                    put("custkey", new Attribute(Type.getClass("long"), 1, "custkey"));
                    put("o_orderstatus", new Attribute(Type.getClass("char"), 2, "orderstatus"));
                    put("o_totalprice", new Attribute(Type.getClass("double"), 3, "totalprice"));
                    put("o_orderdate", new Attribute(Type.getClass("date"), 4, "orderdate"));
                    put("o_orderpriority", new Attribute(Type.getClass("string"), 5, "orderpriority"));
                    put("o_clerk", new Attribute(Type.getClass("string"), 6, "clerk"));
                    put("o_shippriority", new Attribute(Type.getClass("int"), 7, "shippriority"));
                    put("o_comment", new Attribute(Type.getClass("string"), 8, "o_comment"));
                }})
                .withPrimaryKey(Collections.singletonList(ordersPrimaryKey))
                .withParent(lineitem)
                .build();

        Attribute customerPrimaryKey = new Attribute(Type.getClass("long"), 0, "custkey");
        customer = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("custkey", customerPrimaryKey);
                    put("c_name", new Attribute(Type.getClass("string"), 1, "name"));
                    put("c_address", new Attribute(Type.getClass("string"), 2, "address"));
                    put("c_nationkey", new Attribute(Type.getClass("long"), 3, "nationkey"));
                    put("c_phone", new Attribute(Type.getClass("string"), 4, "phone"));
                    put("c_acctbal", new Attribute(Type.getClass("double"), 5, "acctbal"));
                    put("c_mktsegment", new Attribute(Type.getClass("string"), 6, "mktsegment"));
                    put("c_comment", new Attribute(Type.getClass("string"), 7, "c_comment"));
                }})
                .withParent(orders)
                .withPrimaryKey(Collections.singletonList(customerPrimaryKey))
                .build();

        SCHEMAS = ImmutableMap.of(Relation.LINEITEM, lineitem, Relation.ORDERS, orders, Relation.CUSTOMER, customer);
    }

    @Nullable
    public Attribute getColumnAttribute(Relation relation, String columnName) {
        Schema schema = SCHEMAS.get(relation);

        if (schema == null) {
            throw new RuntimeException("Unknown relation name");
        }

        for (Map.Entry<String, Attribute> attributeEntry : schema.getAttributes().entrySet()) {
            if (Attribute.rawColumnName(attributeEntry.getKey()).equals(Attribute.rawColumnName((columnName)))) {
                return attributeEntry.getValue();
            }
        }

        return null;
    }

    @Nullable
    public Schema getSchema(Relation relation) {
        return SCHEMAS.get(relation);
    }

}
