package com.fanpan26.akfak.common.serialization;

import java.io.Closeable;
import java.util.Map;

/**
 * @author fanyuepan
 */
public interface Serializer<T> extends Closeable {

    void configure(Map<String,?> configs,boolean isKey);

    byte[] serialize(String topic,T data);

    @Override
    void close();
}
