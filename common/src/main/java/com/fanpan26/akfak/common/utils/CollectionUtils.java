package com.fanpan26.akfak.common.utils;

import com.fanpan26.akfak.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class CollectionUtils {

    /**
     * group data by topic
     * @param data Data to be partitioned
     * @param <T> Partition data type
     * @return partitioned data
     */
    public static <T> Map<String, Map<Integer, T>> groupDataByTopic(Map<TopicPartition, T> data) {
        Map<String, Map<Integer, T>> dataByTopic = new HashMap<String, Map<Integer, T>>();
        for (Map.Entry<TopicPartition, T> entry: data.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            Map<Integer, T> topicData = dataByTopic.get(topic);
            if (topicData == null) {
                topicData = new HashMap<Integer, T>();
                dataByTopic.put(topic, topicData);
            }
            topicData.put(partition, entry.getValue());
        }
        return dataByTopic;
    }

    /**
     * group partitions by topic
     * @param partitions
     * @return partitions per topic
     */
    public static Map<String, List<Integer>> groupDataByTopic(List<TopicPartition> partitions) {
        Map<String, List<Integer>> partitionsByTopic = new HashMap<String, List<Integer>>();
        for (TopicPartition tp: partitions) {
            String topic = tp.topic();
            List<Integer> topicData = partitionsByTopic.get(topic);
            if (topicData == null) {
                topicData = new ArrayList<Integer>();
                partitionsByTopic.put(topic, topicData);
            }
            topicData.add(tp.partition());
        }
        return  partitionsByTopic;
    }
}
