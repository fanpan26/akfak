package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.TopicPartition;

/**
 * @author fanyuepan
 */
public final class RecordMetadata {

    public static final int UNKNOWN_PARTITION = -1;

    private final long offset;
    private final long timestamp;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final TopicPartition topicPartition;

    private RecordMetadata(TopicPartition topicPartition, long offset, long timestamp, long
            checksum, int serializedKeySize, int serializedValueSize) {
        super();
        this.offset = offset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.topicPartition = topicPartition;
    }

    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset,
                          long timestamp, long checksum, int serializedKeySize, int serializedValueSize) {
        this(topicPartition, baseOffset == -1 ? baseOffset : baseOffset + relativeOffset,
                timestamp, checksum, serializedKeySize, serializedValueSize);
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getChecksum() {
        return checksum;
    }

    public int getSerializedKeySize() {
        return serializedKeySize;
    }

    public int getSerializedValueSize() {
        return serializedValueSize;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }

}
