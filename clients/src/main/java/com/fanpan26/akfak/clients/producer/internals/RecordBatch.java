package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.clients.producer.Callback;
import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.record.MemoryRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息集合，发送消息是批量发送，每一个RecordBatch一个单位
 * @author fanyuepan
 */
public final class RecordBatch {
    private static final Logger logger = LoggerFactory.getLogger(RecordBatch.class);

    /**
     * 消息记录数
     * */
    public int recordCount = 0;
    public int maxRecordSize = 0;
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    public long lastAttemptMs;
    public final MemoryRecords records;
    public final TopicPartition topicPartition;
    public final ProduceRequestResult produceFuture;
    public long lastAppendTime;
    private final List<Thunk> thunks;
    private long offsetCounter = 0L;
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }
}
