package org.hkust.jsonutils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.hkust.objects.*;

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
            List<Expression> scExpressions = makeSelectConditionsExpressions(scMap.entrySet());
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
            Map<String, Object> agMap = (Map<String, Object>) apf.get("AggregateValue");
            Expression agExpression = makeAggregateValueExpression(agMap.entrySet());
            List<AggregateProcessFunction.AggregateValue> aggregateValues = makeAggregateValue(agMap, Collections.singletonList(agExpression));
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

    @VisibleForTesting
    static List<AggregateProcessFunction.AggregateValue> makeAggregateValue(Map<String, Object> avMap, List<Expression> expressions) {
        String type = (String) avMap.get("type");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = new ArrayList<>();
        String aggregateName = (String) avMap.get("name");
        if ("expression".equals(type)) {
            for (Expression expression : expressions) {
                aggregateValues.add(new AggregateProcessFunction.AggregateValue(aggregateName, type, expression));
            }
            return aggregateValues;
        }
        throw new RuntimeException("Unknown AggregateValue type. Currently only supporting expression type. Got: " + type);
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
    static List<Expression> makeSelectConditionsExpressions(Set<Map.Entry<String, Object>> entrySet) {
        List<Expression> expressions = new ArrayList<>();
        for (Map.Entry<String, Object> entry : entrySet) {
            if ((entry.getKey()).startsWith("value")) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                Operator operator = Operator.getOperator((String) value.get("operator"));
                Value left = makeValue((Map<String, Object>) value.get("left_field"));
                Value right = makeValue((Map<String, Object>) value.get("right_field"));
                expressions.add(new Expression(Arrays.asList(left, right), operator));
            }
        }

        return expressions;
    }

    @VisibleForTesting
    static Expression makeAggregateValueExpression(Set<Map.Entry<String, Object>> entrySet) {
        List<Value> values = new ArrayList<>();
        Operator operator = null;
        for (Map.Entry<String, Object> entry : entrySet) {
            if ((entry.getKey()).startsWith("value")) {
                values.add(makeValue((Map<String, Object>) entry.getValue()));
            }
            if ((entry.getKey()).equals("operator")) {
                operator = Operator.getOperator((String) entry.getValue());
            }
        }

        return new Expression(values, operator);
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