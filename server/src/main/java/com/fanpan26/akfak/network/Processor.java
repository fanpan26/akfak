package com.fanpan26.akfak.network;

import com.fanpan26.akfak.common.network.ChannelBuilders;
import com.fanpan26.akfak.common.network.KafkaChannel;
import com.fanpan26.akfak.common.network.LoginType;
import com.fanpan26.akfak.common.network.Mode;
import com.fanpan26.akfak.common.network.SecurityProtocol;
import com.fanpan26.akfak.common.network.Selector;
import com.fanpan26.akfak.common.utils.SystemTime;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private RequestChannel requestChannel;
    private int id;
    private Time time;

    public Processor(int id,
                     int maxRequestSize,
                     RequestChannel requestChannel,
                     long connectionsMaxIdleMs,
                     SecurityProtocol protocol,
                     Map<String,?> channelConfigs) {

        this.id = id;
        this.time = new SystemTime();
        this.requestChannel = requestChannel;

        selector = new Selector(maxRequestSize,
                connectionsMaxIdleMs,
                ChannelBuilders.create(protocol,
                        Mode.SERVER,
                        LoginType.SERVER,
                        channelConfigs,
                        null,
                        true),
                this.time);
    }

    public void accept(SocketChannel channel){
        logger.info("新的连接加入了：{}:{}",channel.socket().getInetAddress(),channel.socket().getPort());
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
            try {
                // 初始化所有的连接，注册OP_READ事件，准备读数据
                configureNewConnections();
                // 处理响应，把响应放入每个Processor响应队列列里，然后发送给客户端
                processNewResponses();
                //轮询处理请求，Selector监听各个SocketChannel，是否有请求发送过来
                poll();
                //处理已经接收完的消息
                processCompletedReceives();
                //处理发送完的消息
                processCompletedSends();
                //处理断开的连接
                processDisconnected();
            } catch (Exception e) {
                logger.error("");
            }
        }

        logger.info("Closing selector - processor " + id);
        shutdownComplete();
    }

    private void processDisconnected() {
        selector.disconnected().forEach(x -> {
            logger.info("{}连接已经断开", x);
        });
    }

    private void processCompletedReceives() {
        selector.completedReceives().forEach(receive -> {
            KafkaChannel channel = selector.channel(receive.source());
            try {
                //只是将请求加入到请求队列，让Handler线程去处理
                requestChannel.sendRequest(new RequestChannel.Request(id, receive.source(), receive.payload()));
                //这里移除OP_READ,等待处理线程处理完毕之后，在添加OP_READ
                selector.mute(receive.source());
            } catch (InterruptedException e) {
                close(selector, receive.source());
                logger.error("Handle request to queue error", e);
            }
        });
    }

    private void processCompletedSends() {
        selector.completedSends().forEach(send -> {
            //添加OP_READ 继续处理
            logger.info("已经完成响应发送，将连接注册OP_READ事件");
            selector.unmute(send.destination());
        });
    }

    private void processNewResponses() {
        RequestChannel.Response response = requestChannel.receiveResponse(id);
        while (response != null) {
            try {
                sendResponse(response);
            } finally {
                response = requestChannel.receiveResponse(id);
            }
        }
    }

    private void sendResponse(RequestChannel.Response response) {
        KafkaChannel channel = selector.channel(response.responseSend.destination());
        if (channel != null) {
            selector.send(response.responseSend);
        }
    }

    private void poll() throws IOException {
        this.selector.poll(500);
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
                logger.info("注册OP_READ事件,{}", connectionId);
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
