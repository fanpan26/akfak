package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author fanyuepan
 */
public interface Selectable {

    int USE_DEFAULT_BUFFER_SIZE = -1;

    void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException;

    /**
     * 唤醒 Selector
     * */
    void wakeup();

    /**
     * 关闭 Selector
     * */
    void close();

    /**
     * 关闭connectionId为id的连接
     * */
    void close(String id);

    /**
     * 发送数据
     * */
    void send(Send send);

    /**
     * I/O 或者连接处理
     * */
    void poll(long timeout) throws IOException;

    /**
     * 已完成发送的数据包
     * */
    List<Send> completedSends();

    /**
     * 已连接的列表
     * */
    List<String> connected();

    /**
     * 断开连接的列表
     * */
    List<String> disconnected();

    /**
     * 停止读数据
     * */
    void mute(String id);

    /**
     * 开始读数据
     * */
    void unmute(String id);

    /**
     * 停止所有连接读取
     * */
    void muteAll();

    /**
     * 恢复所有连接读取
     * */
    void unmuteAll();

    /**
     * channel是否已经连接就绪
     * */
    boolean isChannelReady(String id);

}
