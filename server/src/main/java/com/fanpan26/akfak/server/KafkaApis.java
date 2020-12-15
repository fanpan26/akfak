package com.fanpan26.akfak.server;

import com.fanpan26.akfak.Kafka;
import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.Errors;
import com.fanpan26.akfak.common.requests.MetadataRequest;
import com.fanpan26.akfak.common.requests.MetadataResponse;
import com.fanpan26.akfak.common.requests.ProduceRequest;
import com.fanpan26.akfak.common.requests.ResponseHeader;
import com.fanpan26.akfak.common.requests.ResponseSend;
import com.fanpan26.akfak.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author fanyuepanpub
 *
 */
public class KafkaApis {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApis.class);

    private final RequestChannel requestChannel;

    public KafkaApis(RequestChannel requestChannel) {
        this.requestChannel = requestChannel;
    }

    public void handle(RequestChannel.Request request) {
        logger.info("开始处理请求：{},{}", request.header.toString(), request.body.toString());
        try {
            ApiKeys key = ApiKeys.forId(request.header.apiKey());
            switch (key) {
                case PRODUCE:
                    handleProduceRequest(request);
                    break;
                case METADATA:
                    handleMetadataRequest(request);
                    break;
            }
        } catch (Exception e) {
            logger.error("处理请求异常",e);
        }
    }

    private void handleProduceRequest(RequestChannel.Request request) {
        logger.info("开始处理生产消息请求");

        ProduceRequest produceRequest = (ProduceRequest) request.body;
        Map<TopicPartition, ByteBuffer> records = produceRequest.partitionRecords();
        for (Map.Entry<TopicPartition,ByteBuffer> record : records.entrySet()) {
            TopicPartition tp = record.getKey();
            ByteBuffer buffer = record.getValue();
            logger.info(Arrays.toString(buffer.array()));
        }
    }
    /**
     * 处理 MetadataRequest
     * 需要将 MetadataResponse 发送到响应队列
     */
    private void handleMetadataRequest(RequestChannel.Request request) {
        logger.info("开始处理[MetadataRequest]:{}", request.toString());

        MetadataRequest metadataRequest = (MetadataRequest) request.body;
        List<String> topics = metadataRequest.topics();
        //mock meta data response
        List<Node> nodes = new ArrayList<>();
        Node leader = new Node(0, "127.0.0.1", 9092);
        nodes.add(new Node(0, "127.0.0.1", 9092));
        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();

        List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>();
        partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, leader, nodes, nodes));

        topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE,
                topics.get(0),
                false, partitionMetadata));

        MetadataResponse metadataResponse = new MetadataResponse(nodes, 0, topicMetadata);

        //响应头 id 与请求的 corrrelationId 对应
        ResponseHeader header = new ResponseHeader(request.header.correlationId());
        ResponseSend responseSend = new ResponseSend(request.connectionId, header, metadataResponse);

        RequestChannel.Response response = new RequestChannel.Response(request.processor,request,responseSend);

        try {
            requestChannel.sendResponse(response);
        } catch (InterruptedException e) {
            logger.error("发送响应失败");
        }
    }

}
