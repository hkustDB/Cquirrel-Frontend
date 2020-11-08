package org.hkust.schema;

public enum Relation {
    LINEITEM("lineitem"),
    ORDERS("orders"),
    CUSTOMER("customer");

    private String relation;

    Relation(String relation) {
        this.relation = relation;
    }

    public String getValue() {
        return relation;
    }

    public static Relation getRelation(String relation) {
        for (Relation r : values()) {
            if (r.getValue().equals(relation)) {
                return r;
            }
        }
        throw new IllegalArgumentException();
    }
}
