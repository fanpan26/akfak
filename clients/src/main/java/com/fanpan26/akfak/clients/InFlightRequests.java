package com.fanpan26.akfak.clients;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanyuepan
 */
final class InFlightRequests {

    private final int maxInFlightRequestsPerConnection;
    private final Map<String,Deque<ClientRequest>> requests = new HashMap<>();

    public InFlightRequests(int maxInFlightRequestsPerConnection){
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }


    public boolean canSendMore(String node){
        Deque<ClientRequest> queue = requests.get(node);
        //如果queue为NULL或者空 或者queue的第一个消息已经发送完毕，并且queue的大小不大于最大限制值
        return queue == null ||
                queue.isEmpty() ||
                (queue.peekFirst().request().completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }
}
