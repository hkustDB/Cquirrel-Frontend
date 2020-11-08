package org.hkust.schema;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

class Schema {
    private Map<String, Attribute> attributes;
    private List<Attribute> primaryKey;
    private Schema parent;

    private Schema() {
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder(new Schema());
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public List<Attribute> getPrimaryKey() {
        return primaryKey;
    }

    public Schema getParent() {
        return parent;
    }

    private void setParent(Schema parent) {
        this.parent = parent;
    }

    private void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = attributes;
    }

    private void setPrimaryKey(List<Attribute> primaryKey) {
        this.primaryKey = primaryKey;
    }

    static class SchemaBuilder {
        private Schema schema;

        private SchemaBuilder(Schema schema) {
            this.schema = schema;
        }

        SchemaBuilder withAttributes(Map<String, Attribute> attributes) {
            requireNonNull(attributes);
            schema.setAttributes(ImmutableMap.copyOf(attributes));

            return this;
        }

        SchemaBuilder withParent(Schema parent) {
            schema.setParent(parent);

            return this;
        }

        SchemaBuilder withPrimaryKey(List<Attribute> primaryKey) {
            requireNonNull(primaryKey);
            if (primaryKey.isEmpty()) {
                throw new RuntimeException("Primary key list cannot be empty");
            }
            schema.setPrimaryKey(primaryKey);

            return this;
        }

        Schema build() {
            return schema;
        }

    }

}
