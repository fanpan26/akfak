package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.clients.ClientRequest;
import com.fanpan26.akfak.clients.ClientResponse;
import com.fanpan26.akfak.clients.KafkaClient;
import com.fanpan26.akfak.clients.Metadata;
import com.fanpan26.akfak.clients.RequestCompletionHandler;
import com.fanpan26.akfak.common.Cluster;
import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.network.ByteBufferSend;
import com.fanpan26.akfak.common.network.RequestSend;
import com.fanpan26.akfak.common.network.Send;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.record.Record;
import com.fanpan26.akfak.common.requests.ProduceRequest;
import com.fanpan26.akfak.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    private void run(long now) {
        //获取集群信息，刚开始为空
        Cluster cluster = metadata.fetch();
        //检查服务节点信息，看看是否符合发送条件
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        //存在不知道Leader节点的TopicPartition，需要更新元数据信息
        if (result.unknownLeadersExist) {
            logger.info("Update metadata");
            this.metadata.requestUpdate();
        }
        //移除未准备好的Node
        Iterator<Node> nodeIterator = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (nodeIterator.hasNext()) {
            Node node = nodeIterator.next();
            //检查 Node 是否已经就绪
            if (!this.client.ready(node, now)) {
                nodeIterator.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        //组织数据 NodeId:消息集合
        Map<Integer, List<RecordBatch>> batches = this.accumulator.drain(cluster,
                result.readyNodes,
                this.maxRequestSize,
                now);

        if (batches.size() >0) {
            logger.info("batch-size:{}", batches.get(0).size());
        }
        //如果要保证顺序发送
        if (guaranteeMessageOrder){
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList) {
                    //相当于临时锁住 topicPartition，下次获取batch的时候不会从被锁住的topicPartition获取
                    this.accumulator.mutePartition(batch.topicPartition);
                }
            }
        }

        //放弃发送已经过期的batch,源码中是记录错误，此处先忽略
        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        if (expiredBatches.size() > 0){
            logger.warn("Batches expired,size:{}",expiredBatches.size());
        }

        //包装ClientRequest
        List<ClientRequest> requests = createProduceRequests(batches, now);

        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (result.readyNodes.size() > 0) {
            logger.info("Nodes with data ready to send: {}", result.readyNodes);
            logger.info("Created {} produce requests: {}", requests.size(), requests);
            pollTimeout = 0;
        }
        //遍历ClientRequest 发送消息
        for (ClientRequest request : requests) {
            client.send(request, now);
        }

        // if some partitions are already ready to be sent, the select time would be 0;
        // otherwise if some partition already has some data accumulated but not ready yet,
        // the select time will be the time difference between now and its linger expiry time;
        // otherwise the select time will be the time difference between now and the metadata expiry time;
        this.client.poll(pollTimeout, now);
    }

    private List<ClientRequest> createProduceRequests(Map<Integer,List<RecordBatch>> collated,long now){
        List<ClientRequest> requests = new ArrayList<>(collated.size());
        for (Map.Entry<Integer,List<RecordBatch>> entry : collated.entrySet()){
            requests.add(produceRequest(now,entry.getKey(),acks,requestTimeout,entry.getValue()));
        }
        return requests;
    }

    /**
     * 构建一个ClientRequest
     * @param now 当前时间
     * @param destination 目标服务器
     * @param acks acks参数
     * @param timeout 超时时间
     * @param batches 消息包
     * */
    private ClientRequest produceRequest(long now,int destination,short acks,int timeout,List<RecordBatch> batches){
        Map<TopicPartition,ByteBuffer> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition,RecordBatch> recordsByPartition= new HashMap<>(batches.size());

        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records.buffer());
            recordsByPartition.put(tp, batch);
        }

        ProduceRequest request = new ProduceRequest(acks,timeout,produceRecordsByPartition);

        RequestSend send = new RequestSend(Integer.toString(destination),
                this.client.nextRequestHeader(ApiKeys.PRODUCE),
                request.toStruct());

        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        return new ClientRequest(now, acks != 0, send, callback);
    }

    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, RecordBatch> batches, long now){

    }

    public void wakeup() {
        this.client.wakeup();
    }
}
