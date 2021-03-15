package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;

import static java.util.Objects.requireNonNull;

public class AggregateValue implements Value {
    private final String name;
    //private final String type;
    private final Value value;
    private final Operator aggregation;
    private final Class<?> valueType;

    public AggregateValue(String name, final Value value, Operator aggregation, Class<?> valueType) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        //CheckerUtils.checkNullOrEmpty(type, "type");
        this.name = name;
        /*if (!(type.toLowerCase().equals("expression") || type.toLowerCase().equals("attribute") || type.toLowerCase().equals("constant"))) {
            throw new RuntimeException("Only Expression, Attribute and Constant types are supported for AggregateValue");
        }*/
        //this.type = type;
        requireNonNull(value);
        this.value = value;
        requireNonNull(aggregation);
        this.aggregation = aggregation;
        requireNonNull(valueType);
        this.valueType = valueType;

    }

    public String getName() {
        return name;
    }

    public Operator getAggregation() {
        return aggregation;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    /*public String getType() {
        return type;
    }*/

    public Value getValue() {
        return value;
    }
}
