package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.Errors;
import com.fanpan26.akfak.common.protocol.ProtoUtils;
import com.fanpan26.akfak.common.protocol.types.Schema;
import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.record.Record;
import com.fanpan26.akfak.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class ProduceRequest extends AbstractRequest {

    /**
     * 当前Schema 为ProduceRequest
     */
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.PRODUCE.id);

    private static final String ACKS_KEY_NAME = "acks";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_DATA_KEY_NAME = "topic_data";

    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_DATA_KEY_NAME = "data";

    private static final String PARTITION_KEY_NAME = "partition";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    private final short acks;
    private final int timeout;
    private final Map<TopicPartition, ByteBuffer> partitionRecords;


    /**
     * 在构造函数中构造请求体 Struct
     */
    public ProduceRequest(short acks, int timeout, Map<TopicPartition, ByteBuffer> partitionRecords) {
        super(new Struct(CURRENT_SCHEMA));

        //key 为topic
        Map<String, Map<Integer, ByteBuffer>> recordsByTopic = CollectionUtils.groupDataByTopic(partitionRecords);

        struct.set(ACKS_KEY_NAME, acks);
        struct.set(TIMEOUT_KEY_NAME, timeout);

        List<Struct> topicDatas = new ArrayList<>(recordsByTopic.size());
        //组装 topic 信息
        for (Map.Entry<String, Map<Integer, ByteBuffer>> entry : recordsByTopic.entrySet()) {
            Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);

            topicData.set(TOPIC_KEY_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, ByteBuffer> partitionEntry : entry.getValue().entrySet()) {
                ByteBuffer buffer = partitionEntry.getValue().duplicate();
                //data
                Struct part = topicData.instance(PARTITION_DATA_KEY_NAME)
                        //partition
                        .set(PARTITION_KEY_NAME, partitionEntry.getKey())
                        //record_set
                        .set(RECORD_SET_KEY_NAME, buffer);
                partitionArray.add(part);
            }
            topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(TOPIC_DATA_KEY_NAME, topicDatas.toArray());
        this.acks = acks;
        this.timeout = timeout;
        this.partitionRecords = partitionRecords;
    }

    public ProduceRequest(Struct struct) {
        super(struct);
        partitionRecords = new HashMap<>();
        for (Object topicDataObj : struct.getArray(TOPIC_DATA_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicData.getArray(PARTITION_DATA_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                ByteBuffer records = partitionResponse.getBytes(RECORD_SET_KEY_NAME);
                partitionRecords.put(new TopicPartition(topic, partition), records);
            }
        }
        acks = struct.getShort(ACKS_KEY_NAME);
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        if (acks == 0) {
            return null;
        }
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<TopicPartition, ProduceResponse.PartitionResponse>();
        for (Map.Entry<TopicPartition, ByteBuffer> entry : partitionRecords.entrySet()) {
            responseMap.put(entry.getKey(), new ProduceResponse.PartitionResponse(Errors.forException(e).code(), ProduceResponse.INVALID_OFFSET, Record.NO_TIMESTAMP));
        }

        switch (versionId) {
            case 0:
                return new ProduceResponse(responseMap);
            case 1:
            case 2:
                return new ProduceResponse(responseMap, ProduceResponse.DEFAULT_THROTTLE_TIME, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.PRODUCE.id)));
        }
    }


    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public Map<TopicPartition, ByteBuffer> partitionRecords() {
        return partitionRecords;
    }

    public void clearPartitionRecords() {
        partitionRecords.clear();
    }

    public static ProduceRequest parse(ByteBuffer buffer, int versionId) {
        return new ProduceRequest(ProtoUtils.parseRequest(ApiKeys.PRODUCE.id, versionId, buffer));
    }

    public static ProduceRequest parse(ByteBuffer buffer) {
        return new ProduceRequest(CURRENT_SCHEMA.read(buffer));
    }
}
