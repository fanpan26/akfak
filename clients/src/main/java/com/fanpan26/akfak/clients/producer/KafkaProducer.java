package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.clients.Metadata;
import com.fanpan26.akfak.clients.NetworkClient;
import com.fanpan26.akfak.clients.producer.internals.DefaultPartitioner;
import com.fanpan26.akfak.clients.producer.internals.RecordAccumulator;
import com.fanpan26.akfak.clients.producer.internals.Sender;
import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.PartitionInfo;
import com.fanpan26.akfak.common.serialization.Serializer;
import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.record.CompressionType;
import com.fanpan26.akfak.common.serialization.StringSerializer;
import com.fanpan26.akfak.common.utils.KafkaThread;
import com.fanpan26.akfak.common.utils.SystemTime;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
    private final Serializer<String> keySerializer = new StringSerializer() ;
    private final Serializer<String> valueSerializer = new StringSerializer();
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final int requestTimeoutMs;
    //TODO add interceptors
    //private final ProducerInterceptor<K,V> interceptors;


    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs),null,null);
    }

    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties),null,null);
    }

    private KafkaProducer(ProducerConfig config,Serializer<String> keySerializer,Serializer<String> valueSerializer){
            logger.info("Starting kafka producer");

            try {
                this.producerConfig = config;
                this.time = new SystemTime();

                this.clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.incrementAndGet();
                this.partitioner = new DefaultPartitioner();
                //重试间隔5000ms
                long retryBackoffMs = 5000;
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
                //TODO init Accumulator
                this.accumulator = null;
                List<InetSocketAddress> addresses = new ArrayList<>();
                addresses.add(new InetSocketAddress("127.0.0.1", 9092));
                this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());

                NetworkClient client = new NetworkClient();
                this.sender = new Sender();

                String ioThreadName = "kafka-producer-network-thread" + " | " + clientId;
                this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
                this.ioThread.start();
                logger.info("Producer started");
            }catch (Exception e) {

                close(0, TimeUnit.MILLISECONDS, true);
                throw new KafkaException("Failed to construct kafka producer", e);
            }

    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
        return null;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public void close(long timeout, TimeUnit unit) {

    }

    @Override
    public void close() throws IOException {

    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException){

    }
}
