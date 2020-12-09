package com.fanpan26.akfak.clients;

/**
 * @author fanyuepan
 */
final class ClusterConnectionStates {
    private long reconnectBackoffMs;

    public ClusterConnectionStates(long reconnectBackoffMs){
        this.reconnectBackoffMs = reconnectBackoffMs;
    }
}
