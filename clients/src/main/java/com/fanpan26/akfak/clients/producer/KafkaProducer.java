package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.clients.ClientUtils;
import com.fanpan26.akfak.clients.Metadata;
import com.fanpan26.akfak.clients.NetworkClient;
import com.fanpan26.akfak.clients.producer.internals.DefaultPartitioner;
import com.fanpan26.akfak.clients.producer.internals.RecordAccumulator;
import com.fanpan26.akfak.clients.producer.internals.Sender;
import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.PartitionInfo;
import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.errors.ApiException;
import com.fanpan26.akfak.common.errors.InterruptException;
import com.fanpan26.akfak.common.errors.TimeoutException;
import com.fanpan26.akfak.common.errors.TopicAuthorizationException;
import com.fanpan26.akfak.common.network.ChannelBuilder;
import com.fanpan26.akfak.common.network.Selector;
import com.fanpan26.akfak.common.record.Record;
import com.fanpan26.akfak.common.record.Records;
import com.fanpan26.akfak.common.serialization.Serializer;
import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.record.CompressionType;
import com.fanpan26.akfak.common.serialization.StringSerializer;
import com.fanpan26.akfak.common.utils.KafkaThread;
import com.fanpan26.akfak.common.utils.SystemTime;
import com.fanpan26.akfak.common.utils.Time;
import com.fanpan26.akfak.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author fanyuepan
 */
public class KafkaProducer implements Producer<String,String> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private String clientId;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Time time;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<String> valueSerializer = new StringSerializer();
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    //TODO add interceptors
    //private final ProducerInterceptor<K,V> interceptors;


    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    private KafkaProducer(ProducerConfig config, Serializer<String> keySerializer, Serializer<String> valueSerializer) {
        logger.info("Starting kafka producer");

        try {
            this.producerConfig = config;
            this.time = new SystemTime();

            this.clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.incrementAndGet();
            this.partitioner = new DefaultPartitioner();
            //重试间隔5000ms
            long retryBackoffMs = 500;
            long metadataExpireMs = 1200000;
            this.metadata = new Metadata(retryBackoffMs, metadataExpireMs);
            //最大发送消息大小 1M
            this.maxRequestSize = 1024 * 1024;
            //申请内存大小 32M
            this.totalMemorySize = 32 * 1024 * 1024;
            this.compressionType = CompressionType.NONE;
            //阻塞等待时间 2000ms
            this.maxBlockTimeMs = 2000;
            //请求超时时间 1000ms
            this.requestTimeoutMs = 1000;

            this.accumulator = new RecordAccumulator(1024 * 1024, 32 * 1024 * 1024, compressionType, 2000, 2000, time);
            List<InetSocketAddress> addresses = new ArrayList<>();
            addresses.add(new InetSocketAddress("127.0.0.1", 9092));
            this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());

            long connectionMaxIdleMs = 20 * 60 * 1000;
            int maxInFlightRequestsPerConnection = 5;
            long reconnectBackoffMs = 1000;
            int socketSendBuffer = 1024 * 1024;
            int socketReceiveBuffer = 1024 * 1024;
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            Selector selector = new Selector(-1, connectionMaxIdleMs, channelBuilder, this.time);

            NetworkClient client = new NetworkClient(selector, metadata, clientId,
                    maxInFlightRequestsPerConnection,
                    reconnectBackoffMs,
                    socketSendBuffer,
                    socketReceiveBuffer,
                    requestTimeoutMs,
                    time);

            this.sender = new Sender(client, metadata,
                    accumulator,
                    false,
                    maxRequestSize, (short) 0,
                    1,
                    time,
                    clientId,
                    requestTimeoutMs);

            String ioThreadName = "kafka-producer-network-thread" + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            logger.info("Producer started");
        } catch (Exception e) {

            close(0, TimeUnit.MILLISECONDS, true);
            throw new KafkaException("Failed to construct kafka producer", e);
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
        return doSend(record, callback);
    }

    private Future<RecordMetadata> doSend(ProducerRecord<String, String> record, Callback callback) {
        TopicPartition tp = null;
        try {
            long waitedOnMetadataMs = waitOnMetadata(record.topic(), this.maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, this.maxBlockTimeMs - waitedOnMetadataMs);
            byte[] serializedKey = keySerializer.serialize(record.topic(), record.key());
            byte[] serializedValue = valueSerializer.serialize(record.topic(), record.value());

            int partition = partition(record, serializedKey, serializedValue, metadata.fetch());
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);
            tp = new TopicPartition(record.topic(), partition);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();

            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, callback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                this.sender.wakeup();
            }
            return result.future;
        } catch (ApiException e) {
            if (callback != null) {
                callback.onCompletion(null, e);
            }
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            throw e;
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        }
    }

    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize) {
            //RecordTooLargeException
        }
        if (size > totalMemorySize) {
            //RecordTooLargeException
        }
    }

    private int partition(ProducerRecord<String, String> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        if (partition != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
            int lastPartition = partitions.size() - 1;
            if (partition < 0 || partition > lastPartition) {
                throw new IllegalArgumentException(String.format("Invalid partition given with record: %d is not in the range [0...%d].", partition, lastPartition));
            }
            return partition;
        }
        return this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        try {
            waitOnMetadata(topic, this.maxBlockTimeMs);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
        return this.metadata.fetch().partitionsForTopic(topic);
    }

    private long waitOnMetadata(String topic, long maxWaitMs) throws InterruptedException {
        if (!this.metadata.containsTopic(topic)) {
            this.metadata.add(topic);
        }
        if (metadata.fetch().partitionsForTopic(topic) != null) {
            return 0;
        }
        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        while (metadata.fetch().partitionsForTopic(topic) == null) {
            int version = metadata.requestUpdate();
            sender.wakeup();
            metadata.awaitUpdate(version, remainingWaitMs);
            long elapsed = time.milliseconds() - begin;
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            if (metadata.fetch().unauthorizedTopics().contains(topic)) {
                throw new TopicAuthorizationException(topic);
            }
            remainingWaitMs = maxWaitMs - elapsed;
        }
        return time.milliseconds() - begin;
    }

    @Override
    public void close(long timeout, TimeUnit unit) {

    }

    @Override
    public void close() throws IOException {

    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {

    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    private static volatile boolean  isRunning = true;
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        KafkaProducer producer = new KafkaProducer(properties);
        int i=0;
        while (i<10000) {
            producer.send(new ProducerRecord<String, String>("my-topic", "this is a test,this is a test,this is a test,this is a test,this is a test is a test,this is a test,this is a test,this  is a test,this is a test,this is a test,this "), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error("send result:{}", exception.getMessage(), exception);
                        exception.printStackTrace();
                    } else {
                        logger.info("发送消息成功:{}", metadata);
                    }
                }
            });
            i++;
        }
        while (isRunning){

        }

    }
}
