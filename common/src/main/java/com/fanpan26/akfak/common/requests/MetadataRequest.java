package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.Node;
import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.Errors;
import com.fanpan26.akfak.common.protocol.ProtoUtils;
import com.fanpan26.akfak.common.protocol.types.Schema;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author fanyuepan
 */
public class MetadataRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.METADATA.id);
    private static final String TOPIC_KEY_NAME = "topics";

    private static final MetadataRequest ALL_TOPICS_REQUEST = new MetadataRequest((List<String>)null);

    /**
     * 要拉取的Topic信息
     * */
    private final List<String> topics;

    public static MetadataRequest allTopics() {
        return ALL_TOPICS_REQUEST;
    }


    public MetadataRequest(Struct struct) {
        super(struct);

        Object[] topicArray = struct.getArray(TOPIC_KEY_NAME);
        if (topicArray != null) {
            topics = new ArrayList<>();
            for (Object topicObj : topicArray) {
                topics.add((String) topicObj);
            }
        } else {
            topics = null;
        }
    }

    public MetadataRequest(List<String> topics){
        super(new Struct(CURRENT_SCHEMA));

        if (topics == null){
            struct.set(TOPIC_KEY_NAME,null);
        }else{
            struct.set(TOPIC_KEY_NAME,topics.toArray());
        }
        this.topics = topics;
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        List<MetadataResponse.TopicMetadata> topicMetadatas = new ArrayList<>();
        Errors error = Errors.forException(e);
        List<MetadataResponse.PartitionMetadata> partitions = Collections.emptyList();

        if (topics != null) {
            for (String topic : topics) {
                topicMetadatas.add(new MetadataResponse.TopicMetadata(error, topic, false, partitions));
            }
        }

        switch (versionId) {
            case 0:
            case 1:
                return new MetadataResponse(Collections.<Node>emptyList(), MetadataResponse.NO_CONTROLLER_ID, topicMetadatas, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.METADATA.id)));
        }
    }

    public boolean isAllTopics() {
        return topics == null;
    }

    public List<String> topics() {
        return topics;
    }

    public static MetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new MetadataRequest(ProtoUtils.parseRequest(ApiKeys.METADATA.id, versionId, buffer));
    }

    public static MetadataRequest parse(ByteBuffer buffer) {
        return new MetadataRequest(CURRENT_SCHEMA.read(buffer));
    }
}
