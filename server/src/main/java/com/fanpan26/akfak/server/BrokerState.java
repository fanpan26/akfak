package com.fanpan26.akfak.server;

/**
 * @author fanyuepan
 */
public enum BrokerState {
    NOT_RUNNING(1),
    STARTING(2),
    RUNNING_AS_BROKER(3);

    BrokerState(int state){
        this.state = state;
    }

    private int state;

    public int getState() {
        return state;
    }
}
