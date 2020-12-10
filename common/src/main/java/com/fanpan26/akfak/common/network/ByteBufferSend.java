package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * @author fanyuepan
 */
public class ByteBufferSend implements Send {
    @Override
    public String destination() {
        return null;
    }

    @Override
    public boolean completed() {
        return false;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }
}
