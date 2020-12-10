package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.PartitionInfo;
import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.record.CompressionType;
import com.fanpan26.akfak.common.utils.CopyOnWriteMap;
import com.fanpan26.akfak.common.utils.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author fanyuepan
 */
public final class RecordAccumulator {

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition,Deque<RecordBatch>> batches;
    private final IncompleteRecordBatches incomplete;
    private final Set<TopicPartition> muted;
    private int drainIndex;


    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = new BufferPool(totalSize, batchSize, time);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
    }

    /**
     * 查找准备好的节点
     * */
    public ReadyCheckResult ready(Cluster cluster, long nowMs){
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long .MAX_VALUE;
        boolean unknownLeadersExist = false;

        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition,Deque<RecordBatch>> entry : this.batches.entrySet()){
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque  = entry.getValue();

            Node leader = cluster.leaderFor(part);
            if (leader == null){
                unknownLeadersExist = true;
            }else if (!readyNodes.contains(leader) && !muted.contains(part)){
                synchronized (deque){
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null){
                        //重试次数大于0并且从上次重试时间+重试间隔是否大于当前时间
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        //已经等待的时间=当前时间-上次重试时间
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        //最多等待多少时间=如果是重试，则为重试间隔时间，非重试，则为lingerMs
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        //剩余时间为最多等待时间减去已经等待的时间
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        //batch已经满了或者超过一个batch了
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        //已等待时间已经超过了预期等待时间（已经过期了，需要立马发送消息）
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        //是否可以发送数据了：batch满了，过期了，或者已经没有本地内存了或者被关闭了
                        boolean sendAble = full || expired || exhausted || closed || flushInProgress();
                        //如果可以发送数据并且不是重试状态（或者达到重试条件）则准备发送
                        if (sendAble && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            //从所有的等待时间中选择一个最小的 nextReadyCheckDelayMs 参与下一轮对比
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        //返回检查结果 已经准备发送数据的Node，下一次检查是否Ready的时间间隔，是否leader不存在
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeadersExist);
    }

    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }


    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {

        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            int start = drainIndex = drainIndex % parts.size();
            do {


                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                //这里的判断对应 Sender.guaranteeMessageOrder 保证顺序发送，就先不让当前topic的消息往里加了。
                if (!muted.contains(tp)) {
                    Deque<RecordBatch> deque = getDeque(tp);
                    if (deque != null) {
                        synchronized (deque) {
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                //是否稍后重试
                                boolean backOff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                if (!backOff) {
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        //字节数已经超过最大可发送字节了，直接等待下一次发送
                                        break;
                                    }
                                    RecordBatch batch = deque.pollFirst();
                                    batch.records.close();
                                    size += batch.records.sizeInBytes();
                                    ready.add(batch);
                                    batch.drainedMs = now;
                                }
                            }
                        }
                    }
                }
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp){
        return batches.get(tp);
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    //TODO abortExpiredBatches
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now){
        return Collections.emptyList();
    }

    public final static class ReadyCheckResult {

        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final boolean unknownLeadersExist;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, boolean unknownLeadersExist) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeadersExist = unknownLeadersExist;
        }
    }

    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }

        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }

        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed) {
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
                }
            }
        }

        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }
}
