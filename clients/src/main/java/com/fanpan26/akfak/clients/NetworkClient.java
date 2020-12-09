package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.network.Selectable;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.RequestHeader;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * @author fanyuepan
 */
public class NetworkClient implements KafkaClient {

    private static final Logger logger = LoggerFactory.getLogger(NetworkClient.class);

    private final Selectable selector;
    private final MetadataUpdater metadataUpdater;
    private final Random randOffset;
    private final ClusterConnectionStates connectionStates;
    private final InFlightRequests inFlightRequests;
    private final int socketSendBuffer;
    private final int socketReceiveBuffer;
    private final String clientId;
    private int correlation;
    private final int requestTimeoutMs;
    private final Time time;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(metadata, selector, clientId, maxInFlightRequestsPerConnection,
                reconnectBackoffMs, socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    public NetworkClient(Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this( null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    private NetworkClient(Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time) {

        this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.time = time;
    }

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

    class DefaultMetadataUpdater implements MetadataUpdater{

        public DefaultMetadataUpdater(Metadata metadata){

        }

        @Override
        public List<Node> fetchNodes() {
            return null;
        }

        @Override
        public boolean isUpdateDue(long now) {
            return false;
        }

        @Override
        public long maybeUpdate(long now) {
            return 0;
        }

        @Override
        public boolean maybeHandleDisconnection(ClientRequest request) {
            return false;
        }

        @Override
        public boolean maybeHandleCompletedReceive(ClientRequest request, long now, Struct body) {
            return false;
        }

        @Override
        public void requestUpdate() {

        }
    }
}
