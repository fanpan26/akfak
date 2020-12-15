package com.fanpan26.akfak.clients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fanyuepan
 */
final class ClusterConnectionStates {

    private static final Logger logger = LoggerFactory.getLogger(ClusterConnectionStates.class);

    private long reconnectBackoffMs;
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs){
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<>();
    }


    public boolean canConnect(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //之前没有连接过，直接返回true
        if (state == null) {
            return true;
        } else {
            //连接断开并且尝试重连时间已经到了，所以可以进行再次尝试连接
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
        }
    }

    /**
     * 加入正在连接状态
     */
    public void connecting(String id, long now) {
        logger.info("[{}]正在连接......",id);
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * 断开连接
     * */
    public void disconnected(String id, long now) {
        //如果查不到连接，则说明状态有问题，直接抛出异常
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.DISCONNECTED;
        nodeState.lastConnectAttemptMs = now;
    }

    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null) {
            throw new IllegalStateException("No entry found for connection " + id);
        }
        return state;
    }

    public boolean isConnected(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTED;
    }

    private static class NodeConnectionState {

        ConnectionState state;
        long lastConnectAttemptMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttemptMs = lastConnectAttempt;
        }

        @Override
        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ")";
        }
    }

    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //如果不存在状态，直接连接，不用延迟
        if (state == null) {
            return 0;
        }
        //上次尝试连接的时间距离现在有多久了
        long timeWaited = now - state.lastConnectAttemptMs;
        //如果状态为断开连接，则返回剩余时间，如果剩余时间为负数，则返回0
        if (state.state == ConnectionState.DISCONNECTED) {
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        } else {
            //如果是其他状态，那么就等着吧，没必要进行重试连接
            return Long.MAX_VALUE;
        }
    }

    public boolean isBlackOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        //还没有连接服务器，所以还可以尝试连接，
        if (state == null) {
            return false;
        }
        //服务状态为断开连接，并且还没有到重试连接时间
        return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    public void connected(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CONNECTED;
        logger.info("[{}]连接成功......",id);
    }
}
