/**
 * Created by tom on 12/9/2020.
 * Copyright (c) 2020 tom
 */
public class AttributeValue extends Value {

    private final String name;

    public AttributeValue(String name, DataType type) {
        super(type);
        this.name = name;
    }

    public String getAttributeName() {
        return this.name;
    }

    /*
        Maybe allow directly return the attribute value based on the attribute name and type?
     */
}
