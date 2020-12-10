package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.network.Selectable;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.RequestHeader;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
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
        //当前不在元数据加载过程，并且不需要进入更新元数据状态，并且可以发送请求
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    private boolean canSendRequest(String node){
        //节点状态是已连接
        return connectionStates.isConnected(node) &&
                //selector状态是ready
                selector.isChannelReady(node) &&
                //可以发送更多的消息，此值取决于 maxRequestInFlight
                inFlightRequests.canSendMore(node);
    }

    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty()) {
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        }
        if (isReady(node,now)){
            return true;
        }
        //达到可以连接的条件
        if (connectionStates.canConnect(node.idString(),now)){
            //则尝试连接一下，等待下一次发送消息
            initiateConnect(node, now);
        }
        return false;
    }

    /**
     * Node就是在这段代码中连接的
     * */
    private void initiateConnect(Node node,long now) {
        String nodeConnectionId = node.idString();
        try {

            this.connectionStates.connecting(nodeConnectionId, now);
            //连接Broker
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(node.host(), node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);

        } catch (IOException e) {
            connectionStates.disconnected(nodeConnectionId, now);
            //可能是元数据的问题，所以要更新一下元数据
            metadataUpdater.requestUpdate();
        }
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    @Override
    public boolean connectionFailed(Node node) {
        return false;
    }

    //TODO send
    @Override
    public void send(ClientRequest request, long now) {

    }

    //TODO poll
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

    @Override
    public void close() throws IOException {

    }

    class DefaultMetadataUpdater implements MetadataUpdater{

        private final Metadata metadata;
        private boolean metadataFetchInProgress;

        public DefaultMetadataUpdater(Metadata metadata){
            this.metadata = metadata;
            metadataFetchInProgress = false;
        }

        @Override
        public List<Node> fetchNodes() {
            return null;
        }

        /**
         * 是否马上进入更新状态
         * */
        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
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
