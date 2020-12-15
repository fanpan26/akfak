package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author fanyuepan
 */
public class Selector implements Selectable {

    private static final Logger logger = LoggerFactory.getLogger(Selector.class);
    /**
     * NIO Selector
     */
    private final java.nio.channels.Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final List<NetworkReceive> completedReceives;
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    private final int maxReceiveSize;
    private final long connectionsMaxIdleNanos;
    private final List<Send> completedSends;
    private final List<String> connected;
    private final List<String> disconnected;
    private final List<String> failedSends;
    private final Map<String, Long> lruConnections;
    private final ChannelBuilder channelBuilder;
    private final Set<SelectionKey> immediatelyConnectedKeys;
    private final Time time;
    private long currentTimeNanos;


    public Selector(int maxReceiveSize, long connectionMaxIdleMs, ChannelBuilder channelBuilder, Time time) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        this.maxReceiveSize = maxReceiveSize;
        this.connectionsMaxIdleNanos = connectionMaxIdleMs * 1000 * 1000;
        this.channels = new HashMap<>();
        this.completedSends = new ArrayList<>();
        this.channelBuilder = channelBuilder;
        this.stagedReceives = new HashMap<>();
        this.completedReceives = new ArrayList<>();
        this.connected = new ArrayList<>();
        this.failedSends = new ArrayList<>();
        this.disconnected = new ArrayList<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.lruConnections = new HashMap<>();
        this.time = time;
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id)) {
            throw new IllegalStateException("There is already a connection for id:" + id);
        }

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setSendBufferSize(sendBufferSize);
        }
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE) {
            socket.setReceiveBufferSize(receiveBufferSize);
        }
        socket.setTcpNoDelay(true);

        boolean connected;
        try {
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw e;
        }
        //监听OP_CONNECT
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);

        this.channels.put(id, channel);

        if (connected) {
            logger.info("已经连接服务器:{}", id);
            immediatelyConnectedKeys.add(key);
            key.interestOps(0);
        }
    }

    /**
     * 将socketChannel 与连接 id 绑定
     */
    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {
        //注册 OP_READ 事件
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        //生成一个KafkaChannel绑定到key上
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);
        //存储连接id与channel的关系
        this.channels.put(id, channel);
    }

    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections) {
            close(id);
        }
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            logger.error("Exception closing nioSelector", e);
        }
        channelBuilder.close();
    }

    @Override
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            close(channel);
        }
    }

    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.error("Exception closing connection to node {}:", channel.id(), e);
        }

        this.channels.remove(channel.id());
        //TODO
        //this.stagedReceives.remove(channel);
        //this.lruConnections.remove(channel.id());
    }

    @Override
    public void send(Send send) {
        logger.info("发送消息："+send.toString());
        KafkaChannel channel = channelOrFail(send.destination());
        try {
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
            close(channel);
        }
    }

    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }

        clear();
        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty()) {
            timeout = 0;
        }

        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        currentTimeNanos = endSelect;

        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);
            pollSelectionKeys(immediatelyConnectedKeys, true);
        }
        addToCompletedReceives();

        maybeCloseOldestConnection();
    }

    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iterator = this.stagedReceives.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iterator.next();
                KafkaChannel channel = entry.getKey();
                //这里对应处理逻辑中的 selector.mute(receive.source());
                if (!channel.isMute()) {
                    Deque<NetworkReceive> deque = entry.getValue();
                    NetworkReceive networkReceive = deque.poll();
                    this.completedReceives.add(networkReceive);
                    if (deque.isEmpty()) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute()) {
                return true;
            }
        }
        return false;
    }

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys, boolean isImmediatelyConnected) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            KafkaChannel channel = channel(key);
            lruConnections.put(channel.id(), currentTimeNanos);

            try {
                //可连接的key，要完成连接
                if (isImmediatelyConnected || key.isConnectable()) {
                    if (channel.finishConnect()) {
                        this.connected.add(channel.id());
                    } else {
                        continue;
                    }
                }

                 /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                }

                //可读
                if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
                    NetworkReceive networkReceive;
                    while ((networkReceive = channel.read()) != null) {
                        addToStagedReceives(channel, networkReceive);
                    }
                }

                //可写
                if (channel.ready() && key.isWritable()) {
                    Send send = channel.write();
                    if (send != null) {
                        logger.info("发送消息完毕：{}",send.toString());
                        this.completedSends.add(send);
                    }
                }

                //invalid
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException) {
                    logger.error("Connection with {} disconnected", desc, e);
                } else {
                    logger.error("Unexpected error from {}; closing connection", desc, e);
                }
                //关闭连接
                close(channel);
                this.disconnected.add(channel.id());
            }
        }
    }

    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel)) {
            stagedReceives.put(channel, new ArrayDeque<>());
        }
        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    private int select(long ms) throws IOException {
        if (ms < 0L) {
            throw new IllegalArgumentException("timeout should be >= 0");
        }
        if (ms == 0L) {
            return this.nioSelector.selectNow();

        }
        return this.nioSelector.select(ms);
    }


    private void maybeCloseOldestConnection() {

    }

    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }


    @Override
    public List<Send> completedSends() {
        return completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return completedReceives;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public List<String> disconnected() {
        return disconnected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = channelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    private KafkaChannel channelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null) {
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + id + " existing connections " + channels.keySet());
        }
        return channel;
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values()) {
            mute(channel);
        }
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values()) {
            unmute(channel);
        }
    }

    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }
}
