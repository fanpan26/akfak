package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.exceptions.KafkaException;

import java.nio.channels.SelectionKey;
import java.util.Map;

/**
 * @author fanyuepan
 */
public interface ChannelBuilder {

    /**
     * 配置
     * */
    void configure(Map<String,?> configs) throws KafkaException;


    /**
     * returns a Channel with TransportLayer and Authenticator configured.
     * @param  id  channel id
     * @param  key SelectionKey
     * @param  maxReceiveSize
     * @return KafkaChannel
     */
    KafkaChannel buildChannel(String id, SelectionKey key,int maxReceiveSize) throws KafkaException;

    /**
     * close ChannelBuilder
     * */
    void close();
}
