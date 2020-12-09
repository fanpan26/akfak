package com.fanpan26.akfak.network;

import com.fanpan26.akfak.common.exceptions.KafkaException;
import com.fanpan26.akfak.common.network.ChannelBuilders;
import com.fanpan26.akfak.common.network.KafkaChannel;
import com.fanpan26.akfak.common.network.LoginType;
import com.fanpan26.akfak.common.network.Mode;
import com.fanpan26.akfak.common.network.SecurityProtocol;
import com.fanpan26.akfak.common.network.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author fanyuepan
 */
public class Processor extends AbstractServerThread {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    /**
     * 存储连接的队列
     * */
    private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();

    private Selector selector;
    private int id;

    public Processor(int id,
                     int maxRequestSize,
                     RequestChannel requestChannel,
                     long connectionsMaxIdleMs,
                     SecurityProtocol protocol,
                     Map<String,?> channelConfigs) {

        this.id = id;

        selector = new Selector(maxRequestSize,
                connectionsMaxIdleMs,
                ChannelBuilders.create(protocol,
                        Mode.SERVER,
                        LoginType.SERVER,
                        channelConfigs,
                        null,
                        true));
    }

    public void accept(SocketChannel channel){
        newConnections.add(channel);
        wakeup();
    }

    /**
     * Processor 核心入口，开始处理请求
     * */
    @Override
    public void run() {
        //等待acceptor启动完毕
        startupComplete();
        while (isRunning()) {
            logger.info("waiting for new connections");
            try {
                // 初始化所有的连接，注册OP_READ事件，准备读数据
                configureNewConnections();
//                // 处理响应，把响应放入每个Processor响应队列列里，然后发送给客户端
//                processNewResponses();
//                //轮询处理请求，Selector监听各个SocketChannel，是否有请求发送过来
//                poll();
//                //处理已经接收完的消息
//                processCompletedReceives();
//                //处理发送完的消息
//                processCompletedSends();
//                //处理断开的连接
//                processDisconnected();
            } catch (Exception e) {

            }
        }

        logger.info("Closing selector - processor " + id);
        shutdownComplete();
    }


    /**
     * 从 newConnections 拿到所有的新连接
     * 1.生成唯一连接id
     * 2.注册OP_READ事件，准备读取数据
     * */
    private void configureNewConnections() {
        while (!newConnections.isEmpty()) {
            SocketChannel channel = newConnections.poll();
            String localHost = channel.socket().getLocalAddress().getHostAddress();
            int localPort = channel.socket().getLocalPort();
            String remoteHost = channel.socket().getInetAddress().getHostAddress();
            int remotePort = channel.socket().getPort();

            String connectionId = connectionId(localHost, localPort, remoteHost, remotePort);

            try {
                selector.register(connectionId, channel);
            } catch (ClosedChannelException e) {
                close(channel);
            }
        }
    }

    private String connectionId(String localHost,int localPort,String remoteHost,int remotePort){
        return String.format("%s-%d-%s-%d",localHost,localPort,remoteHost,remotePort);
    }

    @Override
    void wakeup() {
        selector.wakeup();
    }


    public int getId() {
        return id;
    }
}
