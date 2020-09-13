import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonParser {
    private static final Gson gson = new Gson();

    public static Node parse(final String jsonPath) throws Exception {
        String jsonString = new String(Files.readAllBytes(Paths.get(jsonPath)));
        Map map = gson.fromJson(jsonString, Map.class);

        return new Node(makeRelationProcessFunction(
                (Map) map.get("RelationProcessFunction")),
                makeAggregateProcessFunction((Map) map.get("AggregateProcessFunction"))
        );
    }

    private static RelationProcessFunction makeRelationProcessFunction(Map rpfMap) throws Exception {
        RelationProcessFunction rpf = new RelationProcessFunction(
                (String) rpfMap.get("name"),
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
                makeAggregateValue((LinkedTreeMap) apfMap.get("AggregateValue")),
                Operator.getOperator((String) apfMap.get("aggregation")),
                Type.getClass((String) apfMap.get("value_type"))
        );

        return apf;
    }

    private static Expression makeAggregateValue(LinkedTreeMap avMap) throws Exception {
        String type = (String) avMap.get("type");
        List<Value> values = new ArrayList<>();
        if ("expression".equals(type)) {
            Operator operator = Operator.getOperator((String) avMap.get("operator"));
            for (Object entryObject : avMap.entrySet()) {
                Map.Entry entry = (Map.Entry) entryObject;
                String name = (String) entry.getKey();
                if (name.startsWith("value")) {
                    LinkedTreeMap entryMap = (LinkedTreeMap) entry.getValue();
                    String valueType = (String) entryMap.get("type");
                    if (valueType.equals("attribute")) {
                        values.add(new AttributeValue((String) entryMap.get("name")));
                    } else if (valueType.equals("constant")) {
                        values.add(ConstantValue.newInstance((String) entryMap.get("value"), (String) entryMap.get("var_type"), (String) entryMap.get("column_name")));
                    } else {
                        throw new RuntimeException("Unknown type for AggregateValue." + name + ", currently only supporting attribute and constant types. Got: ");
                    }
                }
            }
            return new Expression(values, operator);
        }
        throw new RuntimeException("Unknown AggregateValue type. Currently only supporting expression type. Got: " + type);
    }

    private static List<SelectCondition> makeSelectConditions(LinkedTreeMap scMap) throws Exception {
        List<SelectCondition> selectConditions = new ArrayList<>();
        for (Object entryObject : scMap.entrySet()) {
            Map.Entry entry = (Map.Entry) entryObject;
            String name = (String) entry.getKey();
            LinkedTreeMap value = (LinkedTreeMap) entry.getValue();
            Operator operator = Operator.getOperator((String) value.get("operator"));
            Value left = makeValue((LinkedTreeMap) value.get("left_field"), name);
            Value right = makeValue((LinkedTreeMap) value.get("right_field"), name);
            SelectCondition sc = new SelectCondition(operator, left, right);
            selectConditions.add(sc);
        }

        return selectConditions;
    }

    private static Value makeValue(LinkedTreeMap field, String name) throws Exception {
        String type = (String) field.get("type");
        Value value;
        if (type.equals("attribute")) {
            value = new AttributeValue(name);
        } else if (type.equals("constant")) {
            value = ConstantValue.newInstance((String) field.get("value"), (String) field.get("var_type"), name);
        } else {
            throw new RuntimeException("Unknown field type " + type + " for " + name);
        }

        return value;
    }
}