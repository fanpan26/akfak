package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.TopicPartition;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class ProduceRequest extends AbstractRequest {

    //TODO
    public ProduceRequest(short acks, int timeout, Map<TopicPartition,ByteBuffer> partitionRecords){
        super(new Struct());
    }
}
