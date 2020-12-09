package com.fanpan26.akfak.clients;

/**
 * @author fanyuepan
 */
final class InFlightRequests {
    private int maxInFlightRequestsPerConnection;

    public InFlightRequests(int maxInFlightRequestsPerConnection){
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }
}
