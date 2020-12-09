package com.fanpan26.akfak.network;

import com.fanpan26.akfak.cluster.EndPoint;
import com.fanpan26.akfak.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author fanyuepan
 */
public class Acceptor extends AbstractServerThread {

    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private EndPoint endPoint;
    private int sendBufferSize;
    private int receiveBufferSize;
    private int brokerId;
    private List<Processor> processors;
    private Selector nioSelector;
    private ServerSocketChannel serverSocketChannel;

    public Acceptor(EndPoint endPoint,
                    int sendBufferSize,
                    int receiveBufferSize,
                    int brokerId,
                    List<Processor> processors) {

        this.endPoint = endPoint;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.brokerId = brokerId;
        this.processors = processors;

        try {
            nioSelector = Selector.open();
            serverSocketChannel = openServerSocket(endPoint.getHost(), endPoint.getPort());
            synchronized (this) {
                //遍历所有的processor，启动processor线程
                for (Processor processor : processors) {
                    String threadName = String.format("kafka-network-thread-%d-%s-%d",
                            brokerId,
                            endPoint.getProtocol().toString(),
                            processor.getId());
                    logger.info("Start processor thread:{}",threadName);
                    Utils.newThread(threadName, processor, false).start();
                }
            }
        } catch (IOException e) {
            logger.error("Selector.open error", e);
        }
    }

    /**
     * 核心处理入口 Acceptor 开始接受和处理新的请求
     * */
    @Override
    public void run() {
        try {
            serverSocketChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            logger.error("Channel closed");
        }
        startupComplete();
        //轮询processor进行处理
        try {
            int currentProcessorIndex = 0;
            while (isRunning()) {
                try {
                    //阻塞500ms
                    int ready = nioSelector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = nioSelector.selectedKeys();
                        Iterator<SelectionKey> iterator = keys.iterator();
                        while (iterator.hasNext() && isRunning()) {
                            try {
                                SelectionKey key = iterator.next();
                                iterator.remove();
                                if (key.isAcceptable()) {
                                    accept(key, processors.get(currentProcessorIndex));
                                } else {
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                                }
                                //轮询下一个processor处理
                                currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();
                            } catch (Exception e) {
                                logger.error("Error while accepting connection", e);
                            }
                        }
                    }
                } catch (IOException e) {
                    logger.error("IO Exception while select", e);
                }
            }
        } finally {
            try {
                serverSocketChannel.close();
            } catch (Exception e) {
            }
            try {
                nioSelector.close();
            } catch (Exception e) {
            }
            shutdownComplete();
        }
    }

    /**
     * 由Processor处理 SelectionKeys.ACCEPT
     * */
    private void accept(SelectionKey key,Processor processor) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        //connectionQuotas.inc(socketChannel.socket().getInetAddress)
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);
        socketChannel.socket().setSendBufferSize(sendBufferSize);

        processor.accept(socketChannel);
    }

    private ServerSocketChannel openServerSocket(String host,int port) throws IOException {
        InetSocketAddress address = (host == null || host.trim().isEmpty()) ? new InetSocketAddress(port)
                : new InetSocketAddress(host, port);

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);

        serverSocketChannel.bind(address);
        logger.info("Awaiting socket connections on {}:{}", address.getHostString(), serverSocketChannel.socket().getLocalPort());
        return serverSocketChannel;
    }

    @Override
    void wakeup() {
        nioSelector.wakeup();
    }


}
