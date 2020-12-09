package com.fanpan26.akfak.common.network;

/**
 * @author fanyuepan
 */
public enum LoginType {
    CLIENT("KafkaClient"),
    SERVER("KafkaServer");

    private final String contextName;

    LoginType(String contextName){
        this.contextName = contextName;
    }

    public String getContextName() {
        return contextName;
    }
}
