package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * @author fanyuepan
 */
public interface Send {

    /**
     * 消息发送目的地
     * */
    String destination();

    /**
     * 是否已经发送完毕
     * */
    boolean completed();

    /**
     * 写消息到 channel
     * */
    long writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * 发送的数据包大小
     * */
    long size();
}
