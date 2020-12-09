package com.fanpan26.akfak.server;

import com.fanpan26.akfak.cluster.EndPoint;
import com.fanpan26.akfak.common.network.SecurityProtocol;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class KafkaConfig {


    private int brokerId;
    private int sendBufferSize;
    private int receiveBufferSize;
    private int numNetworkThreads;
    private int queuedMaxRequests;
    private int socketRequestMaxSize;
    private long connectionsMaxIdleMs;
    private int maxConnectionPerIp;

    private Map<String,Object> values;

    private Map<SecurityProtocol,EndPoint> listeners;

    public static KafkaConfig defaultConfig() {
        KafkaConfig config = new KafkaConfig();
        config.setBrokerId(0);
        config.setConnectionsMaxIdleMs(1000);
        config.setMaxConnectionPerIp(5);
        config.setNumNetworkThreads(3);
        config.setReceiveBufferSize(1024 * 1024);
        config.setSendBufferSize(1024 * 1024);
        config.setSocketRequestMaxSize(1024 * 1024);
        config.setConnectionsMaxIdleMs(10 * 60 * 1000);
        config.setQueuedMaxRequests(5);
        Map<SecurityProtocol, EndPoint> listeners = new HashMap<>();
        listeners.put(SecurityProtocol.PLAINTEXT, new EndPoint("127.0.0.1", 9092, SecurityProtocol.PLAINTEXT));
        config.setListeners(listeners);
        return config;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public Map<SecurityProtocol, EndPoint> getListeners() {
        return listeners;
    }

    public void setListeners(Map<SecurityProtocol, EndPoint> listeners) {
        this.listeners = listeners;
    }

    public int getSocketRequestMaxSize() {
        return socketRequestMaxSize;
    }

    public void setSocketRequestMaxSize(int socketRequestMaxSize) {
        this.socketRequestMaxSize = socketRequestMaxSize;
    }

    public long getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs;
    }

    public void setConnectionsMaxIdleMs(long connectionsMaxIdleMs) {
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }

    public int getNumNetworkThreads() {
        return numNetworkThreads;
    }

    public void setNumNetworkThreads(int numNetworkThreads) {
        this.numNetworkThreads = numNetworkThreads;
    }

    public int getQueuedMaxRequests() {
        return queuedMaxRequests;
    }

    public void setQueuedMaxRequests(int queuedMaxRequests) {
        this.queuedMaxRequests = queuedMaxRequests;
    }

    public int getMaxConnectionPerIp() {
        return maxConnectionPerIp;
    }

    public void setMaxConnectionPerIp(int maxConnectionPerIp) {
        this.maxConnectionPerIp = maxConnectionPerIp;
    }

    public Map<String, ?> values() {
        values = new HashMap<>(2);
        return new RecordingMap<>(values);
    }

    private class RecordingMap<V> extends HashMap<String,V>{
        RecordingMap(){}

        RecordingMap(Map<String, ? extends V> m) {
            super(m);
        }
        @Override
        public V get(Object key) {
//            if (key instanceof String) {
//                ignore((String) key);
//            }
            return super.get(key);
        }

    }


}
