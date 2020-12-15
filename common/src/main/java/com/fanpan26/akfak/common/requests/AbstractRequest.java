package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.protocol.ApiKeys;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public abstract class AbstractRequest extends AbstractRequestResponse {

    public AbstractRequest(Struct struct) {
        super(struct);
    }

    public abstract AbstractRequestResponse getErrorResponse(int versionId, Throwable e);

    public static AbstractRequest getRequest(int requestId, int versionId, ByteBuffer buffer) {
        ApiKeys apiKey = ApiKeys.forId(requestId);
        switch (apiKey) {
            case PRODUCE:
                return ProduceRequest.parse(buffer, versionId);
            case METADATA:
                return MetadataRequest.parse(buffer, versionId);
            default:
                throw new AssertionError(String.format("ApiKey %s is not currently handled in `getRequest`, the " +
                        "code should be updated to do so.", apiKey));
        }
    }
}
