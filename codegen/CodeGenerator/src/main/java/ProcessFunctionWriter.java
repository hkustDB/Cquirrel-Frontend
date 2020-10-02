import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class ProcessFunctionWriter implements ClassWriter {
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

    void expressionToCode(final Expression expression, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(expression);
        List<Value> values = expression.getValues();
        valueToCode(values.get(0), code);
        code.append(expression.getOperator().getValue());
        valueToCode(values.get(1), code);
    }

    void valueToCode(Value value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        if (value instanceof ConstantValue) {
            constantValueToCode((ConstantValue) value, code);
        } else if (value instanceof AttributeValue) {
            attributeValueToCode((AttributeValue) value, code);
        } else {
            throw new RuntimeException("Unknown type of value, expecting either ConstantValue or AttributeValue");
        }
    }

    void constantValueToCode(ConstantValue value, StringBuilder code) {
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

    void attributeValueToCode(AttributeValue value, StringBuilder code) {
        requireNonNull(code);
        requireNonNull(value);
        final String columnName = value.getName();
        final Class type = RelationSchema.getColumnAttribute(columnName.toLowerCase()).getType();
        code.append("value(\"")
                .append(columnName.toUpperCase())
                .append("\")")
                .append(".asInstanceOf[")
                .append(type.equals(Type.getClass("date")) ? type.getName() : type.getSimpleName())
                .append("]");
    }
}
