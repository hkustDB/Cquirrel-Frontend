package org.hkust.jsonutils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.hkust.objects.*;
import org.hkust.schema.Relation;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("unchecked")
public class JsonParser {
    private static final Gson gson = new Gson();

    public static Node parse(final String jsonPath) throws Exception {
        String jsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        Map<String, Object> map = gson.fromJson(jsonString, new TypeToken<Map<String, Object>>() {
        }.getType());

        List<Map<String, Object>> rpfMap = (List<Map<String, Object>>) map.get("RelationProcessFunction");
        List<RelationProcessFunction> rpfs = makeRelationProcessFunctions(rpfMap);

        List<Map<String, Object>> apfMap = (List<Map<String, Object>>) map.get("AggregateProcessFunction");
        List<AggregateProcessFunction> apfs = makeAggregateProcessFunctions(apfMap);

        return new Node(rpfs, apfs);
    }

    private static List<RelationProcessFunction> makeRelationProcessFunctions(List<Map<String, Object>> rpfList) {
        List<RelationProcessFunction> result = new ArrayList<>();
        rpfList.forEach(rpf -> {
            Map<String, Object> scMap = (Map<String, Object>) rpf.get("select_conditions");
            List<Expression> scExpressions = makeSelectConditionsExpressions((List<Map<String, Object>>) scMap.get("values"));
            List<SelectCondition> selectConditions = makeSelectConditions(scMap, scExpressions);
            result.add(makeRelationProcessFunction(rpf, selectConditions));
        });

        return result;
    }

    @VisibleForTesting
    static RelationProcessFunction makeRelationProcessFunction(Map<String, Object> rpfMap, List<SelectCondition> selectConditions) {

        return new RelationProcessFunction(
                (String) rpfMap.get("name"),
                (String) rpfMap.get("relation"),
                (List<String>) rpfMap.get("this_key"),
                (List<String>) rpfMap.get("next_key"),
                //Data type coming from json is a double
                ((Double) rpfMap.get("child_nodes")).intValue(),
                (boolean) rpfMap.get("is_Root"),
                (boolean) rpfMap.get("is_Last"),
                (Map<String, String>) rpfMap.get("rename_attribute"),
                selectConditions
        );
    }

    private static List<AggregateProcessFunction> makeAggregateProcessFunctions(List<Map<String, Object>> apfList) {
        List<AggregateProcessFunction> result = new ArrayList<>();
        apfList.forEach(apf -> {
            List<Map<String, Object>> agMap = (List<Map<String, Object>>) apf.get("AggregateValue");
            List<AggregateProcessFunction.AggregateValue> aggregateValues = makeAggregateValues(agMap);
            result.add(makeAggregateProcessFunction(apf, aggregateValues));
        });

        return result;
    }


    @VisibleForTesting
    static AggregateProcessFunction makeAggregateProcessFunction(Map<String, Object> apfMap, List<AggregateProcessFunction.AggregateValue> aggregateValues) {

        return new AggregateProcessFunction(
                (String) apfMap.get("name"),
                (List<String>) apfMap.get("this_key"),
                (List<String>) apfMap.get("next_key"),
                //Currently this assumes that there is exactly 1 AggregateValue, we may have more than one
                aggregateValues,
                Operator.getOperator((String) apfMap.get("aggregation")),
                Type.getClass((String) apfMap.get("value_type"))
        );
    }

    private static List<AggregateProcessFunction.AggregateValue> makeAggregateValues(List<Map<String, Object>> aggregateValues) {
        List<AggregateProcessFunction.AggregateValue> result = new ArrayList<>();
        for (Map<String, Object> aggValue : aggregateValues) {
            Expression agExpression = makeAggregateValueExpression((List<Map<String, Object>>) aggValue.get("values"), (String) aggValue.get("operator"));
            result.add(makeAggregateValue(aggValue, agExpression));
        }

        return result;
    }

    @VisibleForTesting
    static AggregateProcessFunction.AggregateValue makeAggregateValue(Map<String, Object> avMap, Expression expression) {
        String type = (String) avMap.get("type");
        if (!"expression".equals(type)) {
            throw new RuntimeException("Unknown AggregateValue type. Currently only supporting expression type. Got: " + type);

        }
        String aggregateName = (String) avMap.get("name");
        Relation relation = Relation.valueOf((String) avMap.get("relation"));
        return new AggregateProcessFunction.AggregateValue(aggregateName, type, expression, relation);
    }

    @VisibleForTesting
    static List<SelectCondition> makeSelectConditions(Map<String, Object> scMap, List<Expression> expressions) {
        List<SelectCondition> selectConditions = new ArrayList<>();
        Operator nextOperator = Operator.getOperator((String) scMap.get("operator"));
        for (Expression expression : expressions) {
            SelectCondition sc = new SelectCondition(expression, nextOperator);
            selectConditions.add(sc);
        }

        return selectConditions;
    }

    @VisibleForTesting
    static List<Expression> makeSelectConditionsExpressions(List<Map<String, Object>> values) {
        List<Expression> expressions = new ArrayList<>();
        for (Map<String, Object> value : values) {
            Operator operator = Operator.getOperator((String) value.get("operator"));
            Value left = makeValue((Map<String, Object>) value.get("left_field"));
            Value right = makeValue((Map<String, Object>) value.get("right_field"));
            expressions.add(new Expression(Arrays.asList(left, right), operator));
        }

        return expressions;
    }

    @VisibleForTesting
    static Expression makeAggregateValueExpression(List<Map<String, Object>> valuesList, String operator) {
        List<Value> values = new ArrayList<>();
        for (Map<String, Object> value : valuesList) {
            values.add(makeValue(value));
        }

        return new Expression(values, Operator.getOperator(operator));
    }


    private static Value makeValue(Map<String, Object> field) {
        String type = (String) field.get("type");
        Value value;
        if (type.equals("attribute")) {
            String name = (String) field.get("name");
            value = new AttributeValue(name);
        } else if (type.equals("constant")) {
            value = new ConstantValue((String) field.get("value"), (String) field.get("var_type"));
        } else {
            throw new RuntimeException("Unknown field type " + type);
        }

        return value;
    }
}