package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.network.NetworkReceive;
import com.fanpan26.akfak.common.network.RequestSend;
import com.fanpan26.akfak.common.network.Selectable;
import com.fanpan26.akfak.common.network.Send;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.Errors;
import com.fanpan26.akfak.common.protocol.ProtoUtils;
import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.MetadataRequest;
import com.fanpan26.akfak.common.requests.MetadataResponse;
import com.fanpan26.akfak.common.requests.RequestHeader;
import com.fanpan26.akfak.common.requests.ResponseHeader;
import com.fanpan26.akfak.common.utils.Time;
import com.fanpan26.akfak.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        this(null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
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

    private boolean canSendRequest(String node) {
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
        if (isReady(node, now)) {
            return true;
        }
        //达到可以连接的条件
        if (connectionStates.canConnect(node.idString(), now)) {
            //则尝试连接一下，等待下一次发送消息
            initiateConnect(node, now);
        }
        return false;
    }

    /**
     * Node就是在这段代码中连接的
     */
    private void initiateConnect(Node node, long now) {
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

    @Override
    public void send(ClientRequest request, long now) {
        String nodeId = request.request().destination();
        if (!canSendRequest(nodeId)){
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        doSend(request, now);
    }

    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            logger.error("Unexpected error during I/O", e);
        }

        long updateNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        //处理发送完成的请求
        handleCompletedSends(responses, updateNow);
        //处理已经完成的链接
        handleConnections();
        //处理接收完成的响应
        handleCompletedReceives(responses, updateNow);

        return responses;
    }

    private void handleCompletedReceives(List<ClientResponse> responses,long now){
        for (NetworkReceive receive : this.selector.completedReceives()){
            String source = receive.source();
            ClientRequest request = inFlightRequests.completeNext(source);
            Struct body = parseResponse(receive.payload(),request.request().header());
            if (!metadataUpdater.maybeHandleCompletedReceive(request, now, body)) {
                responses.add(new ClientResponse(request, now, false, body));
            }
        }
    }

    public static Struct parseResponse(ByteBuffer responseBuffer,RequestHeader requestHeader){
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);

        short apiKey = requestHeader.apiKey();
        short apiVersion = requestHeader.apiVersion();

        Struct responseBody = ProtoUtils.responseSchema(apiKey,apiVersion).read(responseBuffer);
        //判断请求correlationId = 响应 correlationId
        correlate(requestHeader,responseHeader);
        return responseBody;
    }

    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + ")");
        }
    }


    private void handleCompletedSends(List<ClientResponse> responses,long now) {
        for (Send send : selector.completedSends()) {
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse()) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(new ClientResponse(request, now, false, null));
            }
        }
    }

    /**
     * 处理已经连接完成的请求
     * */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            this.connectionStates.connected(node);
        }
    }

    @Override
    public void close(String nodeId) {

    }


    @Override
    public Node leastLoadedNode(long now) {
        //找到负载最小的Node节点
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inFlight = Integer.MAX_VALUE;
        Node found = null;
        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            //找出这个Node节点下正在发送的请求个数
            int currInFlight = this.inFlightRequests.inFlightRequestCount(node.idString());
            //如果没有在发送消息，并且是连接状态,就你了
            if (currInFlight == 0 && this.connectionStates.isConnected(node.idString())) {
                return node;
            }
            //判断该节点是否可以连接，并且取inFlight最小的作为负载最低的node
            if (!this.connectionStates.isBlackOut(node.idString(), now) && currInFlight < inFlight) {
                inFlight = currInFlight;
                found = node;
            }
        }
        return found;
    }

    @Override
    public int inFlightRequestCount() {
        return inFlightRequests.inFlightRequestCount();
    }

    @Override
    public int inFlightRequestCount(String nodeId) {
        return inFlightRequests.inFlightRequestCount(nodeId);
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id,clientId,correlation++);
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id, version, clientId, correlation++);
    }

    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void close() throws IOException {

    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /**
         * 元信息
         */
        private final Metadata metadata;

        /**
         * 正在fetchMetada中
         */
        private boolean metadataFetchInProgress;

        /**
         * 上次一个broker都不可用的时间
         */
        private long lastNoNodeAvailableMs;

        public DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            metadataFetchInProgress = false;
            this.lastNoNodeAvailableMs = 0;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        /**
         * 是否马上进入更新状态
         */
        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        @Override
        public long maybeUpdate(long now) {
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            //上次没有有效节点的时间+重试时间如果大于现在，说明还没到刷新时间，否则立马刷新
            long timeToNextReconnectAttempt = Math.max(this.lastNoNodeAvailableMs + metadata.refreshBackoff() - now, 0);
            //如果已经进入刷新流程，那么就继续等待
            long waitForMetadataFetch = this.metadataFetchInProgress ? Integer.MAX_VALUE : 0;
            //从这几个时间内找到最大的等待时间
            long metadataTimeout = Math.max(Math.max(timeToNextMetadataUpdate, timeToNextReconnectAttempt), waitForMetadataFetch);
            //如果需要立即刷新Metadata，则从负载最小的Node去拿数据
            if (metadataTimeout == 0) {
                Node node = leastLoadedNode(now);
                maybeUpdate(now, node);
            }
            return metadataTimeout;
        }

        @Override
        public boolean maybeHandleDisconnection(ClientRequest request) {
            return false;
        }

        @Override
        public boolean maybeHandleCompletedReceive(ClientRequest request, long now, Struct body) {
            short apiKey = request.request().header().apiKey();
            if (apiKey == ApiKeys.METADATA.id && request.isInitiatedByNetworkClient()){
                logger.info("收到[MetadataResponse]，更新Metadata");
                handleResponse(request.request().header(),body,now);
                return true;
            }
            return false;
        }

        private void handleResponse(RequestHeader header,Struct body,long now) {
            this.metadataFetchInProgress = false;
            MetadataResponse response = new MetadataResponse(body);
            Cluster cluster = response.cluster();

            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty()) {
                logger.error("Error while fetching metadata with correlation id {} : {}", header.correlationId(), errors);
            }

            if (cluster.nodes().size() > 0) {
                this.metadata.update(cluster, now);
            } else {
                this.metadata.failedUpdate(now);
            }
            logger.info("更新Metadata完毕：{}", this.metadata.fetch().toString());
        }

        @Override
        public void requestUpdate() {

        }

        private void maybeUpdate(long now, Node node) {
            //理论上至少有一个Node，如果为null，说明目前一个Node都不可用
            if (node == null) {
                this.lastNoNodeAvailableMs = now;
                return;
            }
            String nodeConnectionId = node.idString();
            //出于可以发送消息阶段
            if (canSendRequest(nodeConnectionId)) {
                this.metadataFetchInProgress = true;
                //这里是核心逻辑：执行发送FetchRequest的地方
                MetadataRequest metadataRequest;
                if (metadata.needMetadataForAllTopics()) {
                    metadataRequest = MetadataRequest.allTopics();
                }
                else {
                    metadataRequest = new MetadataRequest(new ArrayList<>(metadata.topics()));
                }
                ClientRequest clientRequest = request(now, nodeConnectionId, metadataRequest);
                doSend(clientRequest, now);
            } else if (connectionStates.canConnect(nodeConnectionId, now)) {
                //如果可以连接，那么直接连服务器
                initiateConnect(node, now);
            } else {
                //一个可用的都没有
                this.lastNoNodeAvailableMs = now;
            }
        }
    }

    private ClientRequest request(long now,String node,MetadataRequest metadataRequest) {
        RequestSend send = new RequestSend(node, nextRequestHeader(ApiKeys.METADATA), metadataRequest.toStruct());
        return new ClientRequest(now, true, send, null, true);
    }

    private void doSend(ClientRequest request,long now) {
        request.setSendTimeMs(now);
        this.inFlightRequests.add(request);
        selector.send(request.request());
    }
}
