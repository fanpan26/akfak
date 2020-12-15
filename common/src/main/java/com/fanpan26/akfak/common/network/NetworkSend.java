package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.ResponseHeader;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer... buffers) {
        super(destination, sizeDelimit(buffers));
    }

    public static ByteBuffer[] sizeDelimit(ByteBuffer[] buffers) {
        int size = 0;
        for (int i = 0; i < buffers.length; i++) {
            size += buffers[i].remaining();
        }
        ByteBuffer[] delimited = new ByteBuffer[buffers.length + 1];
        delimited[0] = ByteBuffer.allocate(4);
        delimited[0].putInt(size);
        delimited[0].rewind();
        System.arraycopy(buffers, 0, delimited, 1, buffers.length);
        return delimited;
    }
}
