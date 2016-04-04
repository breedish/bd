package com.epam.bdc;

/**
 * @author zenind
 */
public enum StreamType {

    SITE_IMPRESSION("1"),
    SITE_CLICK("11");

    private final String type;

    StreamType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
