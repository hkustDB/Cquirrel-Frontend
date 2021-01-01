package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;

import static java.util.Objects.requireNonNull;

public class AggregateValue implements Value {
    private final String name;
    private final String type;
    private final Value value;

    public AggregateValue(String name, String type, final Value value) {
        CheckerUtils.checkNullOrEmpty(name, "name");
        CheckerUtils.checkNullOrEmpty(type, "type");
        this.name = name;
        if (!(type.toLowerCase().equals("expression") || type.toLowerCase().equals("attribute"))) {
            throw new RuntimeException("Only Expression & attribute types are supported for AggregateValue");
        }
        this.type = type;
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
