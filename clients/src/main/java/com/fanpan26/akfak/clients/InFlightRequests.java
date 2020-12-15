package com.fanpan26.akfak.clients;

import java.util.ArrayDeque;
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

    public int inFlightRequestCount(String node) {
        Deque<ClientRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    public int inFlightRequestCount() {
        int total = 0;
        for (Deque<ClientRequest> deque : this.requests.values()) {
            total += deque.size();
        }
        return total;
    }

    public boolean canSendMore(String node){
        Deque<ClientRequest> queue = requests.get(node);
        //如果queue为NULL或者空 或者queue的第一个消息已经发送完毕，并且queue的大小不大于最大限制值
        return queue == null ||
                queue.isEmpty() ||
                (queue.peekFirst().request().completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    public void add(ClientRequest request) {
        Deque<ClientRequest> reqs = this.requests.get(request.request().destination());
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(request.request().destination(), reqs);
        }
        reqs.add(request);
    }

    public ClientRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    private Deque<ClientRequest> requestQueue(String node) {
        Deque<ClientRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty()) {
            throw new IllegalStateException("Response from server for which there are no in-flight requests.");
        }
        return reqs;
    }

    public ClientRequest completeLastSent(String node) {
        return requestQueue(node).pollFirst();
    }

    public ClientRequest completeNext(String node) {
        return requestQueue(node).pollLast();
    }
}
