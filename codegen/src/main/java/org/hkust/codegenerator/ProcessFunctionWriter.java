package org.hkust.codegenerator;

import org.hkust.jsonutils.*;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

abstract class ProcessFunctionWriter implements ClassWriter {
    static String keyListToCode(@Nullable List<String> keyList) {
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

    static void expressionToCode(final Expression expression, StringBuilder code) {
        List<Value> values = expression.getValues();
        int size = values.size();
        for (int i = 0; i < size; i++) {
            Value value = values.get(i);
            if (value instanceof Expression) {
                expressionToCode((Expression) value, code);
            } else {
                valueToCode(value, code);
            }
            if (i != size - 1) {
                code.append(expression.getOperator().getValue());
            }
        }
    }

    static void valueToCode(Value value, StringBuilder code) {
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

    static void constantValueToCode(ConstantValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        Class type = value.getType();
        if (type.equals(Type.getClass("date"))) {
            //Needed so generated code parses the date
            code.append("format.parse(\"").append(value.getValue()).append("\")");
        } else if (type.equals(Type.getClass("string"))) {
            code.append("\"").append(value.getValue()).append("\"");
        } else {
            code.append(value.getValue());
        }
    }

    static void attributeValueToCode(AttributeValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        final String columnName = value.getColumnName();
        final Class type = RelationSchema.getColumnAttribute(columnName.toLowerCase()).getType();
        code.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
    }
}
