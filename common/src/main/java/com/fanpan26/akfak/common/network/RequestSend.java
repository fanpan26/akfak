package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.RequestHeader;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public class RequestSend extends NetworkSend {

    private final RequestHeader header;
    private final Struct body;

    public RequestSend(String destination,RequestHeader requestHeader,Struct body) {
        super(destination,serialize(requestHeader,body));
        this.header = requestHeader;
        this.body = body;
    }

    public static ByteBuffer serialize(RequestHeader header, Struct body) {
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        header.writeTo(buffer);
        body.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public RequestHeader header() {
        return this.header;
    }

    public Struct body() {
        return body;
    }

    @Override
    public String toString() {
        return "RequestSend(header=" + header.toString() + ", body=" + body.toString() + ")";
    }

}
