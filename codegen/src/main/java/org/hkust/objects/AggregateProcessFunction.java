package org.hkust.objects;

import com.google.common.collect.ImmutableSet;
import org.hkust.checkerutils.CheckerUtils;
import org.hkust.schema.Attribute;
import org.hkust.schema.RelationSchema;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> outputKey;

    private final List<AggregateValue> aggregateValues;
    private final Operator aggregation;
    private final Class valueType;

    public AggregateProcessFunction(String name, List<String> thisKey, List<String> outputKey, List<AggregateValue> aggregateValues,
                                    Operator aggregation, Class valueType) {
        super(name, thisKey, outputKey);
        this.name = name;
        this.thisKey = thisKey;
        this.outputKey = outputKey;

        requireNonNull(aggregateValues);
        this.aggregateValues = aggregateValues;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;
    }

    @Override
    public Set<Attribute> getAttributeSet(RelationSchema schema) {
        Set<Attribute> result = new HashSet<>();

        aggregateValues.forEach(aggregateValue -> {
            Value value = aggregateValue.getValue();
            if (value instanceof Expression) {
                Expression expression = (Expression) value;
                addExpressionAttributes(expression, result, schema);
            }
        });

        return ImmutableSet.copyOf(result);
    }

    private void addExpressionAttributes(Expression expression, Set<Attribute> attributes, RelationSchema schema) {
        expression.getValues().forEach(val -> {
            addIfAttributeValue(attributes, val, schema);
            if (val instanceof Expression) {
                addExpressionAttributes((Expression)val, attributes, schema);
            }
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Nullable
    public List<String> getThisKey() {
        return thisKey;
    }

    @Nullable
    public List<String> getOutputKey() {
        return outputKey;
    }

    public List<AggregateValue> getAggregateValues() {
        return aggregateValues;
    }

    public Operator getAggregation() {
        return aggregation;
    }

    public Class getValueType() {
        return valueType;
    }

    @Override
    public String toString() {
        return "AggregateProcessFunction{" +
                "name='" + name + '\'' +
                ", thisKey=" + thisKey +
                ", nextKey=" + outputKey +
                ", computation=" + aggregateValues +
                ", aggregation=" + aggregation +
                ", valueType=" + valueType +
                '}';
    }

    public static class AggregateValue {
        private final String name;
        private final String type;
        private final Value value;

        public AggregateValue(String name, String type, final Value value) {
            CheckerUtils.checkNullOrEmpty(name, "name");
            CheckerUtils.checkNullOrEmpty(type, "type");
            this.name = name;
            this.type = type;
            if (!type.toLowerCase().equals("expression")) {
                throw new RuntimeException("Only org.hkust.objects.Expression type is supported for AggregateValue");
            }
            requireNonNull(value);
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public Value getValue() {
            return value;
        }
    }
}
