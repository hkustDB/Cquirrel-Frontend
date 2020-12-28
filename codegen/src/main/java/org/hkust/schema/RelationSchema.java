package org.hkust.schema;

import com.google.common.collect.ImmutableMap;
import org.hkust.objects.Type;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hkust.schema.Attribute.rawColumnName;
import static org.hkust.schema.Relation.*;

public final class RelationSchema {
    public final Schema lineitem;
    public final Schema orders;
    public final Schema customer;
    public final Schema nation;

    private final Map<Relation, Schema> SCHEMAS;

    public RelationSchema() {
        Attribute lineitemPrimaryKey1 = new Attribute(Type.getClass("int"), 3, "linenumber");
        Attribute lineitemPrimaryKey2 = new Attribute(Type.getClass("long"), 0, "orderkey");
        lineitem = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", lineitemPrimaryKey2);
                    put("partkey", new Attribute(Type.getClass("long"), 1, "partkey"));
                    put("suppkey", new Attribute(Type.getClass("long"), 2, "suppkey"));
                    put("linenumber", lineitemPrimaryKey1);
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
                .withChildren(singletonList(ORDERS))
                .withParent(null)
                .withRelationName(LINEITEM)
                .withColumnPrefix("l_")
                .build();

        Attribute ordersPrimaryKey = new Attribute(Type.getClass("long"), 0, "orderkey");
        orders = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("orderkey", ordersPrimaryKey);
                    put("custkey", new Attribute(Type.getClass("long"), 1, "custkey"));
                    put("o_orderstatus", new Attribute(Type.getClass("char"), 2, "o_orderstatus"));
                    put("o_totalprice", new Attribute(Type.getClass("double"), 3, "o_totalprice"));
                    put("o_orderdate", new Attribute(Type.getClass("date"), 4, "o_orderdate"));
                    put("o_orderpriority", new Attribute(Type.getClass("string"), 5, "o_orderpriority"));
                    put("o_clerk", new Attribute(Type.getClass("string"), 6, "o_clerk"));
                    put("o_shippriority", new Attribute(Type.getClass("int"), 7, "o_shippriority"));
                    put("o_comment", new Attribute(Type.getClass("string"), 8, "o_comment"));
                }})
                .withPrimaryKey(singletonList(ordersPrimaryKey))
                .withChildren(singletonList(CUSTOMER))
                .withParent(LINEITEM)
                .withRelationName(ORDERS)
                .withColumnPrefix("o_")
                .build();

        Attribute customerPrimaryKey = new Attribute(Type.getClass("long"), 0, "custkey");
        customer = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("custkey", customerPrimaryKey);
                    put("c_name", new Attribute(Type.getClass("string"), 1, "c_name"));
                    put("c_address", new Attribute(Type.getClass("string"), 2, "c_address"));
                    put("nationkey", new Attribute(Type.getClass("long"), 3, "nationkey"));
                    put("c_phone", new Attribute(Type.getClass("string"), 4, "c_phone"));
                    put("c_acctbal", new Attribute(Type.getClass("double"), 5, "c_acctbal"));
                    put("c_mktsegment", new Attribute(Type.getClass("string"), 6, "c_mktsegment"));
                    put("c_comment", new Attribute(Type.getClass("string"), 7, "c_comment"));
                }})
                .withParent(ORDERS)
                .withPrimaryKey(singletonList(customerPrimaryKey))
                .withChildren(singletonList(NATION))
                .withRelationName(CUSTOMER)
                .withColumnPrefix("c_")
                .build();

        Attribute nationPrimaryKey = new Attribute(Type.getClass("long"), 0, "nationkey");
        nation = Schema.builder()
                .withAttributes(new HashMap<String, Attribute>() {{
                    put("nationkey", nationPrimaryKey);
                    put("n_name", new Attribute(Type.getClass("string"), 1, "n_name"));
                    put("n_regionkey", new Attribute(Type.getClass("string"), 2, "n_address"));
                    put("n_comment", new Attribute(Type.getClass("string"), 3, "n_comment"));
                }})
                .withParent(CUSTOMER)
                .withPrimaryKey(singletonList(nationPrimaryKey))
                .withRelationName(NATION)
                .withColumnPrefix("n_")
                .build();

        SCHEMAS = ImmutableMap.of(LINEITEM, lineitem, ORDERS, orders, CUSTOMER, customer, NATION, nation);
    }

    public Map<Relation, Schema> getAllSchemas() {
        return SCHEMAS;
    }

    @Nullable
    public Attribute getColumnAttributeByRawName(Relation relation, String columnName) {
        Schema schema = SCHEMAS.get(relation);

        if (schema == null) {
            throw new RuntimeException("Unknown relation name");
        }

        for (Map.Entry<String, Attribute> attributeEntry : schema.getAttributes().entrySet()) {
            if (rawColumnName(attributeEntry.getKey()).equals(rawColumnName((columnName)))) {
                return attributeEntry.getValue();
            }
        }

        return null;
    }

    @Nullable
    public Attribute getColumnAttribute(Relation relation, String columnName) {
        Schema schema = SCHEMAS.get(relation);

        if (schema == null) {
            throw new RuntimeException("Unknown relation name");
        }

        return schema.getAttributes().get(columnName);
    }

    public Set<Schema> getAllChildSchemas(List<String> primaryKeyNames) {
        Set<Schema> result = new HashSet<>();
        for (Map.Entry<Relation, Schema> entry : SCHEMAS.entrySet()) {
            Schema schema = entry.getValue();
            List<Attribute> primaryKeys = schema.getPrimaryKey();
            Set<String> pkNames = new HashSet<>();
            primaryKeys.forEach(pk -> pkNames.add(rawColumnName(pk.getName())));
            Set<String> providedNames = new HashSet<>();
            primaryKeyNames.forEach(name -> providedNames.add(rawColumnName(name)));
            if (providedNames.equals(pkNames)) {
                result.add(schema);
                addChildrenRecursively(schema, result);
            }
        }
        return result;
    }

    public void addChildrenRecursively(Schema schema, Set<Schema> children) {
        List<Relation> directChildren = schema.getChildren();
        if (directChildren == null || directChildren.isEmpty()) {
            return;
        }
        List<Schema> childrenSchemas = directChildren.stream().map(SCHEMAS::get).filter(Objects::nonNull).collect(toList());
        children.addAll(childrenSchemas);
        childrenSchemas.forEach(rc -> addChildrenRecursively(rc, children));
    }

    @Nullable
    public Schema getSchema(Relation relation) {
        return SCHEMAS.get(relation);
    }

}
