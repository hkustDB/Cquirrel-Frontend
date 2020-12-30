package org.hkust.codegenerator;

import org.hkust.objects.*;
import org.hkust.schema.Attribute;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.hkust.schema.Schema;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

abstract class ProcessFunctionWriter implements ClassWriter {
    private final RelationSchema relationSchema;

    ProcessFunctionWriter(RelationSchema relationSchema) {
        this.relationSchema = relationSchema;
    }

    protected String keyListToCode(@Nullable List<String> keyList) {
        StringBuilder code = new StringBuilder();
        code.append("Array(");
        if (keyList != null) {
            for (int i = 0; i < keyList.size(); i++) {
                code.append("\"");
                code.append(keyList.get(i).toUpperCase());
                code.append("\"");
                if (i != keyList.size() - 1) {
                    code.append(",");
                }
            }
        }
        code.append(")");

        return code.toString();
    }

    protected void expressionToCode(final Expression expression, StringBuilder code) throws Exception {
        List<Value> values = expression.getValues();
        int size = values.size();
        for (int i = 0; i < size; i++) {
            Value value = values.get(i);
            if (value instanceof Expression) {
                code.append("(");
                expressionToCode((Expression) value, code);
                code.append(")");
            } else {
                valueToCode(value, code);
            }
            if (i != size - 1) {
                code.append(expression.getOperator().getValue());
            }
        }
    }

    protected void valueToCode(Value value, StringBuilder code) throws Exception {
        requireNonNull(code);
        requireNonNull(value);
        //Note: expression can have an expression as one of its values, currently it is not being handled
        if (value instanceof ConstantValue) {
            constantValueToCode((ConstantValue) value, code);
        } else if (value instanceof AttributeValue) {
            attributeValueToCode((AttributeValue) value, code);
        } else {
            throw new RuntimeException("Unknown type of value, expecting either ConstantValue or AttributeValue");
        }
    }

    protected void constantValueToCode(ConstantValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        Class<?> type = value.getType();
        if (type.equals(Type.getClass("date"))) {
            //Needed so generated code parses the date
            code.append("format.parse(\"").append(value.getValue()).append("\")");
        } else if (type.equals(Type.getClass("string"))) {
            code.append("\"").append(value.getValue()).append("\"");
        } else {
            code.append(value.getValue());
        }
    }

    protected void attributeValueToCode(AttributeValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        final String columnName = value.getColumnName();
        final Class<?> type = requireNonNull(relationSchema.getColumnAttributeByRawName(value.getRelation(), columnName.toLowerCase())).getType();
        code.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
    }

    protected List<String> optimizeKey(List<String> rpfNextKeys) {
        if (rpfNextKeys == null || rpfNextKeys.size() < 1) {
            return new ArrayList<>();
        }
        List<String> nextKeys = new ArrayList<>(rpfNextKeys);
        List<String> result = new ArrayList<>();
        Map<Relation, Schema> allSchemas = relationSchema.getAllSchemas();
        for (Map.Entry<Relation, Schema> entry : allSchemas.entrySet()) {
            Schema schema = entry.getValue();
            List<Attribute> primaryKeys = schema.getPrimaryKey();
            List<String> pkNames = primaryKeys.stream().map(Attribute::getName).collect(toList());
            boolean primaryKeysFound = nextKeys.containsAll(pkNames);
            if (primaryKeysFound) {
                result.addAll(pkNames);
                Set<Schema> childSchemas = relationSchema.getAllChildSchemas(pkNames);
                for (Schema cs : childSchemas) {
                    nextKeys.removeIf(nk -> relationSchema.getColumnAttribute(cs.getRelation(), nk) != null);
                }
            }
        }
        result.addAll(nextKeys);
        return result;
    }
}
