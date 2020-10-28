package org.hkust.jsonutils;

public enum Position {
    ROOT("ROOT"),
    LEAF("LEAF");

    private String position;

    Position(String position) {
        this.position = position;
    }

    public String position() {
        return position;
    }
}