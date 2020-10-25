import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JsonParser {
    private static final Gson gson = new Gson();

    public static Node parse(final String jsonPath) throws Exception {
        String jsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        Map<String, Object> map = gson.fromJson(jsonString, Map.class);

        Map<String, Object> rpfMap = (Map) map.get("RelationProcessFunction");
        Map<String, Object> scMap = (LinkedTreeMap) rpfMap.get("select_conditions");
        List<Expression> scExpressions = makeExpressions(scMap.entrySet());
        List<SelectCondition> selectConditions = makeSelectConditions(scMap, scExpressions);


        Map<String, Object> apfMap = (Map) map.get("AggregateProcessFunction");
        Map<String, Object> agMap = (Map) apfMap.get("AggregateValue");
        List<Expression> agExpressions = makeExpressions(agMap.entrySet());
        List<AggregateProcessFunction.AggregateValue> aggregateValues = makeAggregateValue(agMap, agExpressions);

        return new Node(makeRelationProcessFunction(rpfMap, selectConditions),
                makeAggregateProcessFunction(apfMap, aggregateValues)
        );
    }

    @VisibleForTesting
    static RelationProcessFunction makeRelationProcessFunction(Map<String, Object> rpfMap, List<SelectCondition> selectConditions) throws Exception {
        RelationProcessFunction rpf = new RelationProcessFunction(
                (String) rpfMap.get("name"),
                (String) rpfMap.get("relation"),
                (List) rpfMap.get("this_key"),
                (List) rpfMap.get("next_key"),
                ((Integer) rpfMap.get("child_nodes")),
                (boolean) rpfMap.get("is_Root"),
                (boolean) rpfMap.get("is_Last"),
                (Map) rpfMap.get("rename_attribute"),
                selectConditions
        );

        return rpf;
    }

    @VisibleForTesting
    static AggregateProcessFunction makeAggregateProcessFunction(Map<String, Object> apfMap, List<AggregateProcessFunction.AggregateValue> aggregateValues) {
        AggregateProcessFunction apf = new AggregateProcessFunction(
                (String) apfMap.get("name"),
                (List) apfMap.get("this_key"),
                (List) apfMap.get("next_key"),
                //Currently this assumes that there is exactly 1 AggregateValue, we may have more than one
                aggregateValues,
                Operator.getOperator((String) apfMap.get("aggregation")),
                Type.getClass((String) apfMap.get("value_type"))
        );

        return apf;
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
    static List<Expression> makeExpressions(Set<Map.Entry<String, Object>> entrySet) {
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