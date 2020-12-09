package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;

/**
 * @author fanyuepan
 */
public interface KafkaClient extends Closeable {

    boolean isReady(Node node, long now);

    boolean ready(Node node, long now);

    long connectionDelay(Node node, long now);

    boolean connectionFailed(Node node);

    void send(ClientRequest request, long now);

    List<ClientResponse> poll(long timeout, long now);

    void close(String nodeId);

    Node leastLoadedNode(long now);

    int inFlightRequestCount();

    int inFlightRequestCount(String nodeId);

    RequestHeader nextRequestHeader(ApiKeys key);

    RequestHeader nextRequestHeader(ApiKeys key, short version);

    void wakeup();
}
