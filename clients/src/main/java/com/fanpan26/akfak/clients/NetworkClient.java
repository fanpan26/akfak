package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.requests.RequestHeader;

import java.util.List;

/**
 * @author fanyuepan
 */
public class NetworkClient implements KafkaClient {

    @Override
    public boolean isReady(Node node, long now) {
        return false;
    }

    @Override
    public boolean ready(Node node, long now) {
        return false;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    @Override
    public boolean connectionFailed(Node node) {
        return false;
    }

    @Override
    public void send(ClientRequest request, long now) {

    }

    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        return null;
    }

    @Override
    public void close(String nodeId) {

    }

    @Override
    public Node leastLoadedNode(long now) {
        return null;
    }

    @Override
    public int inFlightRequestCount() {
        return 0;
    }

    @Override
    public int inFlightRequestCount(String nodeId) {
        return 0;
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return null;
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return null;
    }

    @Override
    public void wakeup() {

    }
}
