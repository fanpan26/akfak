package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.clients.producer.Partitioner;
import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.PartitionInfo;
import com.fanpan26.akfak.common.utils.Utils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author fanyuepan
 */
public class DefaultPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            int nextValue = counter.getAndIncrement();
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                return toPositive(nextValue) % numPartitions;
            }
        } else {
            return toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {

    }
}
