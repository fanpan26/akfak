package com.fanpan26.akfak.clients.producer;

/**
 * @author fanyuepan
 */
public interface Callback {

    /**
     * 消息发送异步回调
     * @param metadata
     * @param exception
     * */
    void onCompletion(RecordMetadata metadata,Exception exception);

}
