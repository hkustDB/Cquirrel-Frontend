package org.hkust.codegenerator;

import org.hkust.objects.*;
import org.hkust.schema.Relation;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

abstract class ProcessFunctionWriter implements ClassWriter {
    private final RelationSchema schema;

    ProcessFunctionWriter(RelationSchema schema) {
        this.schema = schema;
    }

    String keyListToCode(@Nullable List<String> keyList) {
        StringBuilder code = new StringBuilder();
        code.append("Array(");
        if (keyList != null) {
            for (int i = 0; i < keyList.size(); i++) {
                code.append("\"");
                code.append(keyList.get(i));
                code.append("\"");
                if (i != keyList.size() - 1) {
                    code.append(",");
                }
            }
        }
        code.append(")");

        return code.toString();
    }

    void expressionToCode(Relation relation, final Expression expression, StringBuilder code) throws Exception {
        List<Value> values = expression.getValues();
        int size = values.size();
        for (int i = 0; i < size; i++) {
            Value value = values.get(i);
            if (value instanceof Expression) {
                expressionToCode(relation, (Expression) value, code);
            } else {
                valueToCode(relation, value, code);
            }
            if (i != size - 1) {
                code.append(expression.getOperator().getValue());
            }
        }
    }

    void valueToCode(Relation relation, Value value, StringBuilder code) throws Exception {
        requireNonNull(code);
        requireNonNull(value);
        //Note: expression can have an expression as one of its values, currently it is not being handled
        if (value instanceof ConstantValue) {
            constantValueToCode((ConstantValue) value, code);
        } else if (value instanceof AttributeValue) {
            attributeValueToCode(relation, (AttributeValue) value, code);
        } else {
            throw new RuntimeException("Unknown type of value, expecting either ConstantValue or AttributeValue");
        }
    }

    void constantValueToCode(ConstantValue value, StringBuilder code) {
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

    void attributeValueToCode(Relation relation, AttributeValue value, StringBuilder code) throws Exception {
        requireNonNull(code);
        requireNonNull(value);
        final String columnName = value.getColumnName();
        final Class<?> type = schema.getColumnAttribute(relation, columnName.toLowerCase()).getType();
        code.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
    }
}
