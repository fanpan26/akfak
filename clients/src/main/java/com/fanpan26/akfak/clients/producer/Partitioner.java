package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.Cluster;

/**
 * @author fanyuepan
 */
public interface Partitioner {

    int partition(String topic,Object key,byte[] keyBytes,Object value,byte[] valueBytes,Cluster cluster);

    void close();
}
