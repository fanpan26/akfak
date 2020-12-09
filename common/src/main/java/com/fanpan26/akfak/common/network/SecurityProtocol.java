package com.fanpan26.akfak.common.network;

/**
 * @author fanyuepan
 */
public enum SecurityProtocol {
    PLAINTEXT(0, "PLAINTEXT", false),
    SSL(1, "SSL", false),
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT", false),
    SASL_SSL(3, "SASL_SSL", false),
    TRACE(Short.MAX_VALUE, "TRACE", true);

    SecurityProtocol(int id, String name, boolean isTesting) {
        this.id = id;
        this.name = name;
        this.isTesting = isTesting;
    }

    private int id;
    private String name;
    private boolean isTesting;

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
