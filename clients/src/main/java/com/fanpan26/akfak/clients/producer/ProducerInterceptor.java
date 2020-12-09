package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.Configurable;

/**
 * @author fanyuepan
 */
public interface ProducerInterceptor<K,V> extends Configurable {

    ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);

    void onAcknowledgement(RecordMetadata metadata,Exception exception);

    void close();
}
