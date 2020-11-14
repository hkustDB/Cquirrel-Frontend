package org.hkust.objects;

import org.hkust.checkerutils.CheckerUtils;

import java.util.Objects;

//TODO: revise the generalization
public class ConstantValue<T> implements Value {
    private final T value;
    private final Class type;
    public T getValue() {
        return value;
    }

    public Class getType() {
        return type;
    }

    public ConstantValue(T val, String type) {
        //CheckerUtils.checkNullOrEmpty(val, "val");
        CheckerUtils.checkNullOrEmpty(type, "type");

        String typeLower = type.toLowerCase();
        Class clss = Type.getClass(typeLower);
        if (clss == null) {
            throw new RuntimeException("Unknown data type: " + type);
        }
        this.value = val;
        this.type = clss;
    }

    @Override
    public String toString() {
        return "ConstantValue{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConstantValue)) return false;
        ConstantValue<?> that = (ConstantValue<?>) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }
}
