package org.hkust.jsonutils;

import org.hkust.checkerutils.CheckerUtils;

import java.util.Objects;

public class AttributeValue implements Value {

    private final String columnName;

    public AttributeValue(String columnName) {
        CheckerUtils.checkNullOrEmpty(columnName,"name");
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        return "AttributeValue{" +
                "name='" + columnName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AttributeValue)) return false;
        AttributeValue that = (AttributeValue) o;
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName);
    }

    public String getColumnName() {
        return columnName;
    }
}