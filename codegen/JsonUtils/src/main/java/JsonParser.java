import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JsonParser {
    private static final Gson gson = new Gson();

    public static Node parse(final String jsonPath) throws Exception {
        String jsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        Map map = gson.fromJson(jsonString, Map.class);

        return new Node(makeRelationProcessFunction((Map) map.get("RelationProcessFunction")),
                makeAggregateProcessFunction((Map) map.get("AggregateProcessFunction"))
        );
    }

    private static RelationProcessFunction makeRelationProcessFunction(Map rpfMap) throws Exception {
        RelationProcessFunction rpf = new RelationProcessFunction(
                (String) rpfMap.get("name"),
                (String) rpfMap.get("relation"),
                (List) rpfMap.get("this_key"),
                (List) rpfMap.get("next_key"),
                ((Double) rpfMap.get("child_nodes")).intValue(),
                (boolean) rpfMap.get("is_Root"),
                (boolean) rpfMap.get("is_Last"),
                (Map) rpfMap.get("rename_attribute"),
                makeSelectConditions((LinkedTreeMap) rpfMap.get("select_conditions"))
        );

        return rpf;
    }

    private static AggregateProcessFunction makeAggregateProcessFunction(Map apfMap) throws Exception {
        AggregateProcessFunction apf = new AggregateProcessFunction(
                (String) apfMap.get("name"),
                (List) apfMap.get("this_key"),
                (List) apfMap.get("next_key"),
                //Currently this assumes that there is exactly 1 AggregateValue, we may have more than one
                makeAggregateValue((LinkedTreeMap) apfMap.get("AggregateValue")),
                Operator.getOperator((String) apfMap.get("aggregation")),
                Type.getClass((String) apfMap.get("value_type"))
        );

        return apf;
    }

    private static List<AggregateProcessFunction.AggregateValue> makeAggregateValue(LinkedTreeMap<String, Object> avMap) throws Exception {
        String type = (String) avMap.get("type");
        List<AggregateProcessFunction.AggregateValue> aggregateValues = new ArrayList<>();
        String aggregateName = (String) avMap.get("name");
        if ("expression".equals(type)) {
            List<Value> values = new ArrayList<>();
            Operator operator = Operator.getOperator((String) avMap.get("operator"));
            for (Map.Entry<String, Object> entry : avMap.entrySet()) {
                String name = entry.getKey();
                if (name.startsWith("value")) {
                    LinkedTreeMap<String, Object> entryMap = (LinkedTreeMap<String, Object>) entry.getValue();
                    String valueType = (String) entryMap.get("type");
                    if (valueType.equals("attribute")) {
                        values.add(new AttributeValue((String) entryMap.get("name")));
                    } else if (valueType.equals("constant")) {
                        values.add(new ConstantValue((String) entryMap.get("value"), (String) entryMap.get("var_type")));
                    } else {
                        throw new RuntimeException("Unknown type for AggregateValue." + name + ", currently only supporting attribute and constant types. Got: " + valueType);
                    }
                }
            }
            aggregateValues.add(new AggregateProcessFunction.AggregateValue(aggregateName, type, new Expression(values, operator)));
            return aggregateValues;
        }
        throw new RuntimeException("Unknown AggregateValue type. Currently only supporting expression type. Got: " + type);
    }

    private static List<SelectCondition> makeSelectConditions(LinkedTreeMap<String, Object> scMap) throws Exception {
        List<SelectCondition> selectConditions = new ArrayList<>();
        Operator nextOperator = Operator.getOperator((String) scMap.get("operator"));
        for (Map.Entry<String, Object> entry : scMap.entrySet()) {
            if ((entry.getKey()).startsWith("value")) {
                LinkedTreeMap<String, Object> value = (LinkedTreeMap<String, Object>) entry.getValue();
                Operator operator = Operator.getOperator((String) value.get("operator"));
                Value left = makeValue((LinkedTreeMap<String, Object>) value.get("left_field"));
                Value right = makeValue((LinkedTreeMap<String, Object>) value.get("right_field"));
                SelectCondition sc = new SelectCondition(new Expression(Arrays.asList(left, right), operator), nextOperator);
                selectConditions.add(sc);
            }
        }

        return selectConditions;
    }

    private static Value makeValue(LinkedTreeMap<String, Object> field) throws Exception {
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