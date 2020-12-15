package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.network.NetworkSend;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public class ResponseSend extends NetworkSend {


    public ResponseSend(String destination, ResponseHeader header, AbstractRequestResponse response) {
        this(destination, header, response.toStruct());
    }

    public ResponseSend(String destination, ResponseHeader header, Struct body) {
        super(destination, serialize(header, body));
    }

    public static ByteBuffer serialize(ResponseHeader header, Struct body) {
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        header.writeTo(buffer);
        body.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }
}
