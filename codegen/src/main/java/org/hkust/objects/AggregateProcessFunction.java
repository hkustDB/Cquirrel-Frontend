package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AggregateProcessFunction extends ProcessFunction {
    private final String name;
    @Nullable
    private final List<String> thisKey;
    @Nullable
    private final List<String> nextKey;

    private final List<AggregateValue> aggregateValues;
    private final Operator aggregation;
    private final Class valueType;

    public AggregateProcessFunction(String name, List<String> thisKey, List<String> nextKey, List<AggregateValue> aggregateValues,
                                    Operator aggregation, Class valueType) {
        super(name, thisKey, nextKey);
        this.name = name;
        this.thisKey = thisKey;
        this.nextKey = nextKey;

        requireNonNull(aggregateValues);
        this.aggregateValues = aggregateValues;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;
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
    public List<String> getNextKey() {
        return nextKey;
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
                ", nextKey=" + nextKey +
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
