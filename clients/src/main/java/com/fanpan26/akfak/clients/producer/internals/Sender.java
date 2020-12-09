package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.clients.KafkaClient;
import com.fanpan26.akfak.clients.Metadata;
import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.network.Send;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * /**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 * @author fanyuepan
 */
public class Sender implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private final KafkaClient client;
    private final RecordAccumulator accumulator;
    private final Metadata metadata;
    private final boolean guaranteeMessageOrder;
    private final int maxRequestSize;
    private final short acks;
    private final int retries;
    private final Time time;
    private volatile boolean running;
    private volatile boolean forceClose;
    private String clientId;
    private final int requestTimeout;

    public Sender(KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  Time time,
                  String clientId,
                  int requestTimeout) {
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.clientId = clientId;
        this.requestTimeout = requestTimeout;
    }

    /**
     * 发送消息主线程
     */
    @Override
    public void run() {
        while (running) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                logger.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        try {
            this.client.close();
        } catch (Exception e) {
            logger.error("Failed to close network client", e);
        }
    }

    private void run(long now){
        //获取集群信息，刚开始为空
        Cluster cluster = metadata.fetch();

        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

    }
}
