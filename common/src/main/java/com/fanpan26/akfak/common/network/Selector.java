package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author fanyuepan
 */
public class Selector implements Selectable {

    private static final Logger logger = LoggerFactory.getLogger(Selector.class);
    /**
     *  NIO Selector
     * */
    private final java.nio.channels.Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final int maxReceiveSize;
    private final long connectionsMaxIdleNanos;
    private final List<Send> completedSends;
    private final List<String> connected;
    private final ChannelBuilder channelBuilder;
    private final Set<SelectionKey> immediatelyConnectedKeys;


    public Selector(int maxReceiveSize, long connectionMaxIdleMs,ChannelBuilder channelBuilder) {
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
        this.connected = new ArrayList<>();
        this.immediatelyConnectedKeys = new HashSet<>();
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
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);

        this.channels.put(id, channel);

        if (connected) {
            immediatelyConnectedKeys.add(key);
            key.interestOps(0);
        }
    }

    /**
     * 将socketChannel 与连接 id 绑定
     * */
    public void register(String id,SocketChannel socketChannel) throws ClosedChannelException {
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
        for (String id : connections){
            close(id);
        }
        try{
            this.nioSelector.close();
        }catch (IOException | SecurityException e){
            logger.error("Exception closing nioSelector",e);
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

    }

    @Override
    public void poll(long timeout) throws IOException {

    }

    @Override
    public List<Send> completedSends() {
        return null;
    }

    @Override
    public List<String> connected() {
        return null;
    }

    @Override
    public List<String> disconnected() {
        return null;
    }

    @Override
    public void mute(String id) {

    }

    @Override
    public void unmute(String id) {

    }

    @Override
    public void muteAll() {

    }

    @Override
    public void unmuteAll() {

    }

    @Override
    public boolean isChannelReady(String id) {
        return false;
    }
}
