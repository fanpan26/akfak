package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * @author fanyuepan
 */
public class NetworkReceive implements Receive {
    @Override
    public String source() {
        return null;
    }

    @Override
    public boolean complete() {
        return false;
    }

    @Override
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return 0;
    }
}
