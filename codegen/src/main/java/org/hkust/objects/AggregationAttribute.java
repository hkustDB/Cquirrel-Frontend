package org.hkust.objects;

import static java.util.Objects.requireNonNull;

public class AggregationAttribute implements Value {
    private final String type;
    private final String name;
    private final Class<?> varType;

    public AggregationAttribute(String type, String name, Class<?> varType) {
        requireNonNull(type);
        requireNonNull(name);
        requireNonNull(varType);
        this.type = type;
        this.name = name;
        this.varType = varType;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Class<?> getVarType() {
        return varType;
    }
}
