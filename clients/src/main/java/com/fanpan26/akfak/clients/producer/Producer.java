package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.PartitionInfo;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author fanyuepan
 */
public interface Producer<K,V> extends Closeable {

    Future<RecordMetadata> send(ProducerRecord<K,V> record);

    Future<RecordMetadata> send(ProducerRecord<K,V> record,Callback callback);

    void flush();

    List<PartitionInfo> partitionsFor(String topic);

    void close(long timeout, TimeUnit unit);
}
