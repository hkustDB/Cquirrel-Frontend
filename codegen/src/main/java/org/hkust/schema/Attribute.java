package org.hkust.schema;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Attribute {
    private final Class type;
    private final int position;
    private final String name;

    public Attribute(Class type, int position, String name) {
        requireNonNull(type);
        if (position < 0) {
            throw new RuntimeException("position in Attribute cannot be < 0");
        }
        this.name = name;
        this.type = type;
        this.position = position;
    }

    public Class getType() {
        return type;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Attribute)) return false;
        Attribute attribute = (Attribute) o;
        return position == attribute.position &&
                Objects.equals(type, attribute.type) &&
                Objects.equals(name, attribute.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, position, name);
    }
}
