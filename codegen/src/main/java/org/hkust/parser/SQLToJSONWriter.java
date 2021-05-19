package org.hkust.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectQueryBlock;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGSelectStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * The json-simple library is compiled with an old bytecode version: 46.0, which causes multiple "unchecked" warning.
 * The following command will suppress all such warning as a temporal fixed.  A better solution is to use a library like
 * org.json to support generics type.
 */
@SuppressWarnings("unchecked")
public class SQLToJSONWriter {
    private final JSONObject outputJsonObject = new JSONObject();
    private final HashMap<String, JSONObject> relationJsonObject = new HashMap<>();
    private final HashMap<String, JSONArray> SelectCondition = new HashMap<>();
    private final Map<String, Integer> childCount = new HashMap<>();
    private final JSONArray aggregationFunctions = new JSONArray();
    private final JSONObject aggregationFunction = new JSONObject();
    private final JSONArray aggregateValues = new JSONArray();
    public HashSet<SQLExpr> BinaryPredicates = new HashSet<>();
    public HashMap<String, List<SQLExpr>> UnaryPredicates = new HashMap<>();
    private String outputFileName = "";
    private boolean lineitemSetThisKey = false;
    private String lastObject;
    private String root = "";

    SQLToJSONWriter(String Filename) {
        outputFileName = Filename;
    }

    /***
     *
     * @param Visitor
     * @return
     */
    public boolean addJoinStructure(ExportTableAliasVisitor Visitor) {
        JSONArray messages = new JSONArray();
        HashSet<String> table = Visitor.table;
        if (table.contains("lineitem")) {
            JSONArray keyList = new JSONArray();
            root = "lineitem";
            if (table.contains("orders")) {
                JSONObject l_o = new JSONObject();
                l_o.put("primary", "orders");
                l_o.put("foreign", "lineitem");
                messages.add(l_o);
                int count = childCount.getOrDefault("lineitem", 0);
                childCount.put("lineitem", count + 1);
                keyList.add("orderkey");
                writeRelationJsonObject("lineitem", "this_key", keyList);
                lineitemSetThisKey = true;
            }
            if (table.contains("partsupp")) {
                JSONObject l_ps = new JSONObject();
                l_ps.put("primary", "partsupp");
                l_ps.put("foreign", "lineitem");
                messages.add(l_ps);
                int count = childCount.getOrDefault("lineitem", 0);
                childCount.put("lineitem", count + 1);
            }
            //TODO Handling Part and Supplier if PS table not exists.
            if (table.contains("part")) {
                JSONObject l_p = new JSONObject();
                l_p.put("primary", "part");
                l_p.put("foreign", "lineitem");
                messages.add(l_p);
                int count = childCount.getOrDefault("lineitem", 0);
                childCount.put("lineitem", count + 1);
                keyList.add("partkey");
                writeRelationJsonObject("lineitem", "this_key", keyList);
                lineitemSetThisKey = true;
            }
            if (table.contains("supplier")) {
                JSONObject l_s = new JSONObject();
                l_s.put("primary", "supplier");
                l_s.put("foreign", "lineitem");
                messages.add(l_s);
                int count = childCount.getOrDefault("lineitem", 0);
                childCount.put("lineitem", count + 1);
            }

            writeRelationDefinition("lineitem");
            if (!lineitemSetThisKey) {
                keyList.add("orderkey");
                keyList.add("l_linenumber");
                writeRelationJsonObject("lineitem", "this_key", keyList);
                lineitemSetThisKey = true;
            }
            writeRelationJsonObject("lineitem", "is_Root", true);
            lastObject = "lineitem";
            writeRelationJsonObject("lineitem", "is_Last", true);
        }


        if (table.contains("orders")) {
            JSONArray keyList = new JSONArray();
            JSONArray nextKey = new JSONArray();
            if (root.equals("")) root = "orders";
            if (table.contains("customer")) {
                JSONObject o_c = new JSONObject();
                o_c.put("primary", "customer");
                o_c.put("foreign", "orders");
                messages.add(o_c);
                int count = childCount.getOrDefault("orders", 0);
                childCount.put("orders", count + 1);
                keyList.add("custkey");
            } else {
                keyList.add("orderkey");
            }
            writeRelationJsonObject("orders", "this_key", keyList);
            writeRelationDefinition("orders");
            if (!table.contains("lineitem")) {
                writeRelationJsonObject("orders", "is_Root", true);
                lastObject = "orders";
            } else {
                writeRelationJsonObject("orders", "is_Root", false);
                nextKey.add("orderkey");
                writeRelationJsonObject("orders", "next_key", nextKey);

            }
            writeRelationJsonObject("orders", "is_Last", true);
        }

        if (table.contains("customer")) {
            JSONArray keyList = new JSONArray();
            JSONArray nextKey = new JSONArray();
            if (table.contains("nation")) {
                JSONObject c_n = new JSONObject();
                c_n.put("primary", "nation");
                c_n.put("foreign", "customer");
                messages.add(c_n);
                int count = childCount.getOrDefault("customer", 0);
                childCount.put("customer", count + 1);
                keyList.add("nationkey");
            } else {
                keyList.add("custkey");
            }
            writeRelationJsonObject("customer", "this_key", keyList);

            if (root.equals("")) root = "customer";
            writeRelationDefinition("customer");
            if (!table.contains("orders")) {
                writeRelationJsonObject("customer", "is_Root", true);
                lastObject = "customer";
            } else {
                writeRelationJsonObject("customer", "is_Root", false);
                nextKey.add("custkey");
                writeRelationJsonObject("customer", "next_key", nextKey);
            }
            writeRelationJsonObject("customer", "is_Last", true);
        }

        if (table.contains("nation")) {
            JSONArray keyList = new JSONArray();
            JSONArray nextKey = new JSONArray();
            if (table.contains("region")) {
                JSONObject n_r = new JSONObject();
                n_r.put("primary", "region");
                n_r.put("foreign", "nation");
                messages.add(n_r);
                int count = childCount.getOrDefault("nation", 0);
                childCount.put("nation", count + 1);
                keyList.add("regionkey");
            } else {
                keyList.add("nationkey");
            }
            writeRelationJsonObject("nation", "this_key", keyList);

            if (root.equals("")) root = "nation";
            writeRelationDefinition("nation");
            if (!table.contains("customer") && !table.contains("supplier")) {
                writeRelationJsonObject("nation", "is_Root", true);
                lastObject = "nation";
            } else {
                writeRelationJsonObject("nation", "is_Root", false);
                nextKey.add("nationkey");
                writeRelationJsonObject("nation", "next_key", nextKey);
            }
            writeRelationJsonObject("nation", "is_Last", true);
        }

        if (table.contains("part")) {
            JSONArray keyList = new JSONArray();
            JSONArray nextKey = new JSONArray();
            keyList.add("partkey");
            writeRelationJsonObject("part", "this_key", keyList);
            writeRelationDefinition("part");
            if (root.equals("")) {
                root = "part";
                writeRelationJsonObject("part", "is_Root", false);
                lastObject = "part";
            } else {
                writeRelationJsonObject("part", "is_Root", false);
                nextKey.add("partkey");
                writeRelationJsonObject("part", "next_key", nextKey);
            }
            writeRelationJsonObject("part", "is_Last", true);
        }

        // TODO Handling other relations.

        // Merge the JSON Array into the JSON Object.
        outputJsonObject.put("join_structure", messages);
        return true;
    }

    /***
     * Detected if the query is recursive query, if true, then the query must be like
     * "
     * select * from ([SELECT_QUERY]) as ... where [CONDITION];
     * "
     * @param Visitor
     * @return [SELECT_QUERY] in String
     */
    public String checkIfRecursive(ExportTableAliasVisitor Visitor) {
        if (((PGSelectQueryBlock) Visitor.selectStatement.iterator().next().getSelect().getQuery())
                .getSelectList().iterator().next().getExpr().toString().equals("*")) {
            if (!Visitor.aggregation.isEmpty()) {
                SQLExpr filters = ((SQLSelectQueryBlock) Visitor.selectStatement.iterator().next().getSelect().getQuery()).getWhere();
                if (filters.getClass() == SQLBinaryOpExpr.class) {
                    processFilter((SQLBinaryOpExpr) filters, "aggregation");
                }
            } else {
                System.err.println("No Aggregation in Recursive queries!");
            }
            return ((SQLSubqueryTableSource) ((SQLSelectQueryBlock) Visitor.selectStatement.iterator().next().getSelect().getQuery()).getFrom()).getSelect().getQuery().toString();
        } else {
            return null;
        }
    }

    /***
     * The function adds the RelationProcessFunction section in the final json file.
     * @param Visitor The visitor of the parser.
     * @return
     */
    public boolean addRelationProcessFunction(ExportTableAliasVisitor Visitor) {


        JSONArray relationProcessFunctions = new JSONArray();

        SQLExpr filters = ((SQLSelectQueryBlock) Visitor.selectStatement.iterator().next().getSelect().getQuery()).getWhere();

        if (filters.getClass() == SQLBinaryOpExpr.class) {
            processFilter((SQLBinaryOpExpr) filters);
        }
        SelectCondition.forEach((key, value) -> {
            if (!key.equals("aggregation")) {
                JSONObject condition = new JSONObject();
                condition.put("operator", "&&");
                condition.put("values", value);
                writeRelationJsonObject(key, "select_conditions", condition);
            }
        });

        if (Visitor.groupByAttributes != null && Visitor.groupByAttributes.size() > 0) {
            JSONArray keyList = new JSONArray();
            if (Visitor.groupByAttributes.size() == 1) {
                keyList.add(truncateKey(Visitor.groupByAttributes.get(0).toString()));
            } else {
                Visitor.groupByAttributes.forEach(i -> keyList.add(truncateKey(i.toString())));

            }
            writeRelationJsonObject(lastObject, "next_key", keyList);
            //TODO Modify if handling multiple relations.
            aggregationFunction.put("this_key", keyList);
            aggregationFunction.put("output_key", keyList);
        } else {
            /*Object this_key = relationJsonObject.getOrDefault(lastObject, new JSONObject()).get("this_key");
            writeRelationJsonObject(lastObject, "next_key", this_key);*/
            writeRelationJsonObject(lastObject, "next_key", null);

            aggregationFunction.put("this_key", null);
            aggregationFunction.put("output_key", null);
        }

        relationJsonObject.forEach((key, value) -> relationProcessFunctions.add(value));
        outputJsonObject.put("RelationProcessFunction", relationProcessFunctions);
        return true;
    }

    public boolean addAggregationFunction(ExportTableAliasVisitor Visitor) {

        if (!Visitor.aggregation.isEmpty()) {
            int count = 0;
            aggregationFunction.put("name", "QAggregate" + count);
            aggregationFunction.put("delta_output", true);
            JSONArray aggregateValueList = new JSONArray();
            for (SQLAggregateExpr i : Visitor.aggregation) {
                aggregateValueList.add(getAggregateValue(i));
            }
            aggregationFunction.put("AggregateValue", aggregateValueList);
            //TODO modify to handle multiple aggregates
            if (SelectCondition.containsKey("aggregation")) {
                aggregationFunction.put("OutputSelectCondition", SelectCondition.get("aggregation"));
            }
            aggregationFunctions.add(aggregationFunction);
            outputJsonObject.put("AggregateProcessFunction", aggregationFunctions);

        }

        return true;
    }

    private JSONObject getAggregateValue(SQLAggregateExpr expr) {
        JSONObject aggregate = new JSONObject();
        switch (expr.getMethodName()) {
            case "sum":
                aggregate.put("aggregation", "+");
                break;
            case "count":
                if (expr.getOption().name().equals("DISTINCT")) {
                    aggregate.put("aggregation", "COUNT_DISTINCT");
                } else {
                    aggregate.put("aggregation", "COUNT");
                }
                break;
            default:
        }

        if (((SQLSelectItem) expr.getParent()).getAlias() != null) {
            aggregate.put("name", ((SQLSelectItem) expr.getParent()).getAlias());
            aggregateValues.add(((SQLSelectItem) expr.getParent()).getAlias());
        }

        aggregate.put("value", writeValueObject(expr.getArguments().get(0)));
        switch (expr.computeDataType().getName()) {
            case "bigint":
                aggregate.put("value_type", "int");
                break;
            case "double":
            case "":
                aggregate.put("value_type", "Double");
                break;
            default:
                aggregate.put("value_type", expr.computeDataType());
        }
        return aggregate;
    }

    /***
     * As all relation Json objects are stored in a Map, hence the above function is used to add a key-value pair to the
     * JSONObject for a given relation.
     * @param relation The relation that the JSONObject is added.
     * @param key The key of the JSONObject
     * @param value The value of the JSONObject
     */
    private void writeRelationJsonObject(String relation, String key, Object value) {
        JSONObject temp = relationJsonObject.getOrDefault(relation, new JSONObject());
        temp.put(key, value);
        relationJsonObject.put(relation, temp);
    }

    /***
     *
     * @param expr
     */

    private void processFilter(SQLBinaryOpExpr expr) {
        processFilter(expr, "");
    }

    private void processFilter(SQLBinaryOpExpr expr,
                               String relationNameInit) {
        if (expr.getOperator().name == "OR") {
            JSONArray temp = SelectCondition.getOrDefault(lastObject, new JSONArray());
            temp.add(writeValueObject(expr));
            SelectCondition.put(lastObject, temp);
            List<SQLExpr> tempList = UnaryPredicates.getOrDefault(lastObject, new ArrayList<>());
            tempList.add(expr);
            UnaryPredicates.put(lastObject, tempList);
        } else {
            if (expr.getLeft().getClass() == SQLIdentifierExpr.class || expr.getRight().getClass() == SQLIdentifierExpr.class) {
                if (expr.getLeft().getClass() == SQLIdentifierExpr.class && expr.getRight().getClass() == SQLIdentifierExpr.class) {
                    BinaryPredicates.add(expr);
                    if (getIdentifierRelation((SQLIdentifierExpr) expr.getLeft()).equals(getIdentifierRelation((SQLIdentifierExpr) expr.getRight()))) {
                        String relationName = relationNameInit;
                        if (relationName.equals(""))
                            relationName = getIdentifierRelation((SQLIdentifierExpr) expr.getLeft());
                        JSONArray temp = SelectCondition.getOrDefault(relationName, new JSONArray());
                        temp.add(writeValueObject(expr));
                        SelectCondition.put(relationName, temp);
                    }
                } else {
                    SQLIdentifierExpr identifierExpr;
                    SQLBinaryOperator OP;
                    if (expr.getLeft().getClass() == SQLIdentifierExpr.class) {
                        identifierExpr = (SQLIdentifierExpr) expr.getLeft();
                    } else {
                        identifierExpr = (SQLIdentifierExpr) expr.getRight();
                    }
                    OP = expr.getOperator();
                    String relationName = relationNameInit;
                    if (relationName == "") relationName = getIdentifierRelation(identifierExpr);
                    //writeSelectConditionJSONObject(expr.getLeft(), expr.getRight(), OP, relationName);
                    JSONArray temp = SelectCondition.getOrDefault(relationName, new JSONArray());
                    temp.add(writeValueObject(expr));
                    SelectCondition.put(relationName, temp);
                    List<SQLExpr> tempList = UnaryPredicates.getOrDefault(relationName, new ArrayList<>());
                    tempList.add(expr);
                    UnaryPredicates.put(relationName, tempList);
                }
            } else {
                if (expr.getLeft().getClass() == SQLBinaryOpExpr.class) processFilter((SQLBinaryOpExpr) expr.getLeft());
                else {
                    if (expr.getLeft().getClass() == SQLInListExpr.class) processInList((SQLInListExpr) expr.getLeft());
                }
                if (expr.getRight().getClass() == SQLBinaryOpExpr.class)
                    processFilter((SQLBinaryOpExpr) expr.getRight());
                else {
                    if (expr.getRight().getClass() == SQLInListExpr.class)
                        processInList((SQLInListExpr) expr.getRight());
                }
            }
        }
    }

    private SQLBinaryOpExpr inToBinaryOr(SQLInListExpr expr) {
        SQLIdentifierExpr identifierExpr;
        identifierExpr = (SQLIdentifierExpr) expr.getExpr();
        List<SQLExpr> targetList = expr.getTargetList();
        StringBuilder inToOrClause = new StringBuilder();
        inToOrClause.append("select * where ");
        int cnt = 0;
        for (SQLExpr value : targetList) {
            cnt += 1;
            inToOrClause.append(" ").append(identifierExpr.toString()).append(" = ").append(value.toString());
            if (cnt < targetList.size()) inToOrClause.append(" or ");
        }
        List<SQLStatement> stmtList = SQLUtils.parseStatements(inToOrClause.toString(), JdbcConstants.POSTGRESQL);
        return (SQLBinaryOpExpr) ((PGSelectQueryBlock) ((PGSelectStatement) stmtList.get(0)).getSelect().getQuery()).getWhere();
    }

    private void processInList(SQLInListExpr expr) {

        SQLIdentifierExpr identifierExpr;
        identifierExpr = (SQLIdentifierExpr) expr.getExpr();
        String relationName = getIdentifierRelation(identifierExpr);
        List<SQLExpr> tempList = UnaryPredicates.getOrDefault(relationName, new ArrayList<>());
        tempList.add(expr);
        UnaryPredicates.put(relationName, tempList);
//        List<SQLExpr> tempList = UnaryPredicates.getOrDefault(relationName, new ArrayList<>());
//        tempList.add(expr);
//        UnaryPredicates.put(relationName, tempList);
//        List<SQLExpr> targetList = expr.getTargetList();
//        JSONArray temp = SelectCondition.getOrDefault(relationName, new JSONArray());
//        StringBuilder inToOrClause = new StringBuilder();
//        inToOrClause.append("select * where ");
//        int cnt = 0;
//        for (SQLExpr value : targetList) {
//            cnt +=1;
//            inToOrClause.append(" ").append(identifierExpr.toString()).append(" = ").append(value.toString());
//            if (cnt < targetList.size()) inToOrClause.append(" or ");
//        }
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(inToOrClause.toString(), JdbcConstants.POSTGRESQL);
        //SQLBinaryOpExpr inToOrWhere = (SQLBinaryOpExpr) ((PGSelectQueryBlock) ((PGSelectStatement) stmtList.get(0)).getSelect().getQuery()).getWhere();
        JSONArray temp = SelectCondition.getOrDefault(relationName, new JSONArray());
        SQLBinaryOpExpr inToOrWhere = inToBinaryOr(expr);
        temp.add(writeValueObject(inToOrWhere));
        SelectCondition.put(relationName, temp);
    }

    /***
     * Given a SQL Identifier from TPC-H benchmark, decide its relation based on the prefix in the identifier.
     * @param expr a SQL Identifier
     * @return a relation in String from TPC-H benchmark.
     */
    private String getIdentifierRelation(SQLIdentifierExpr expr) {
        String prefix = expr.getLowerName().split("_")[0];
        switch (prefix) {
            case "l":
                return "lineitem";
            case "o":
                return "orders";
            case "ps":
                return "partsupp";
            case "s":
                return "supplier";
            case "n":
                return "nation";
            case "r":
                return "region";
            case "c":
                return "customer";
            case "p":
                return "part";
            default:
                return ("Unknown");
        }
    }

    /***
     * For a binary operation expression, the function writes the expression into the JSON format inside the
     * [SelectCondition] JSON Array for corresponding relation.
     * @param leftField
     * @param rightField
     * @param OP
     * @param relationName
     */
    private void writeSelectConditionJSONObject(SQLExpr leftField,
                                                SQLExpr rightField,
                                                SQLBinaryOperator OP,
                                                String relationName) {
        JSONObject condition = new JSONObject();
        condition.put("left_field", writeValueObject(leftField));
        condition.put("right_field", writeValueObject(rightField));
        writeOP(OP, condition);

        JSONArray temp = SelectCondition.getOrDefault(relationName, new JSONArray());
        temp.add(condition);
        SelectCondition.put(relationName, temp);
    }

    private void writeOP(SQLBinaryOperator OP, JSONObject object) {
        switch (OP.name) {
            case "=":
                object.put("operator", "==");
                break;
            case "OR":
                object.put("operator", "||");
                break;
            case "AND":
                object.put("operator", "&&");
                break;
            default:
                object.put("operator", OP.name);
        }
    }

    private String truncateKey(String original) {
        if (original.contains("key") && original.contains("_")) {
            return original.split("_")[1];
        } else {
            return original;
        }
    }

    /***
     *
     * @param expr
     * @return
     */
    private JSONObject writeValueObject(SQLExpr expr) {
        JSONObject value = new JSONObject();

        if (expr.getClass() == SQLBinaryOpExpr.class) {
            value.put("type", "expression");
            value.put("left_field", writeValueObject(((SQLBinaryOpExpr) expr).getLeft()));
            value.put("right_field", writeValueObject(((SQLBinaryOpExpr) expr).getRight()));
            writeOP(((SQLBinaryOpExpr) expr).getOperator(), value);
            return value;
        }

        if (expr.getClass() == SQLIdentifierExpr.class) {
            if (getIdentifierRelation((SQLIdentifierExpr) expr) != "Unknown") {
                value.put("type", "attribute");
                value.put("relation", getIdentifierRelation((SQLIdentifierExpr) expr));
                value.put("name", truncateKey(((SQLIdentifierExpr) expr).getLowerName()));
                return value;
            } else {
                value.put("type", "aggregate_attribute");
                value.put("name", truncateKey(((SQLIdentifierExpr) expr).getLowerName()));
                // Currently, for any attributes created during execution, we fix the value type to be Double.
                value.put("var_type", "Double");
                value.put("store_type", "String");
            }
        }

        if (expr.getClass() == SQLDateExpr.class) {
            value.put("type", "constant");
            value.put("var_type", "Date");
            value.put("value", ((SQLDateExpr) expr).getValue());
            return value;
        }

        if (expr.getClass() == SQLCharExpr.class) {
            value.put("type", "constant");
            value.put("var_type", "varchar");
            value.put("value", ((SQLCharExpr) expr).getValue());
            return value;
        }

        if (expr.getClass() == SQLIntegerExpr.class) {
            value.put("type", "constant");
            value.put("var_type", "int");
            value.put("value", ((SQLIntegerExpr) expr).getValue());
            return value;
        }

        if (expr.getClass() == SQLBigIntExpr.class) {
            value.put("type", "constant");
            value.put("var_type", "Long");
            value.put("value", ((SQLBigIntExpr) expr).getValue());
            return value;
        }

        if (expr.getClass() == SQLNumberExpr.class) {
            value.put("type", "constant");
            value.put("var_type", "Double");
            value.put("value", ((SQLNumberExpr) expr).getValue());
        }

        if (expr.getClass() == SQLInListExpr.class) {
            return writeValueObject(inToBinaryOr((SQLInListExpr) expr));
        }

        return value;
    }

    private JSONObject createInformation() {
        JSONObject information = new JSONObject();
        JSONArray relations = new JSONArray();
        relationJsonObject.keySet().forEach(i -> relations.add(i.toString()));
        information.put("relations", relations);
        JSONArray Binary = new JSONArray();
        BinaryPredicates.forEach(i -> Binary.add(i.toString()));
        information.put("binary", Binary);
        JSONArray Unary = new JSONArray();
        for (String i : UnaryPredicates.keySet()) {
            JSONObject temp = new JSONObject();
            JSONArray tempArray = new JSONArray();
            for (SQLExpr j : UnaryPredicates.get(i)) {
                tempArray.add(j.toString());
            }
            temp.put(i, tempArray);
            Unary.add(temp);
        }
        information.put("unary", Unary);
        information.put("aggregation", aggregateValues);
        return information;
    }

    /***
     *
     * @throws Exception
     */
    public void printJson() throws Exception {
        Path p = Paths.get(outputFileName);
        Path q = Paths.get("information.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        String json = gson.toJson(JsonParser.parseString(outputJsonObject.toJSONString()));
        Files.write(p, json.getBytes());
        Gson gson2 = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        JSONObject information = createInformation();
        String json2 = gson2.toJson(JsonParser.parseString(information.toJSONString()));
        Files.write(p.getParent().resolve(q).toAbsolutePath().normalize(), json2.getBytes());
    }

    /***
     *
     * @param name
     */
    private void writeRelationDefinition(String name) {
        JSONObject object = relationJsonObject.getOrDefault(name, new JSONObject());
        object.put("name", "Q" + name);
        object.put("relation", name);
        object.put("child_nodes", childCount.getOrDefault(name, 0));
        object.put("rename_attribute", null);
        relationJsonObject.put(name, object);

    }
}
