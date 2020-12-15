package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.protocol.Protocol;
import com.fanpan26.akfak.common.protocol.types.Field;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public class ResponseHeader extends AbstractRequestResponse {

    private static final Field CORRELATION_KEY_FIELD = Protocol.RESPONSE_HEADER.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct struct) {
        super(struct);
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId){
        super(new Struct(Protocol.RESPONSE_HEADER));
        struct.set(CORRELATION_KEY_FIELD,correlationId);
        this.correlationId = correlationId;
    }

    public int correlationId(){
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(Protocol.RESPONSE_HEADER.read(buffer));
    }
}
