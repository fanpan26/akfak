package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * @author fanyuepan
 */
public interface Receive {

    /**
     * 数据来源
     * */
    String source();

    /**
     * 读取完成
     * */
    boolean complete();

    /**
     * 读取数据
     * */
    long readFrom(ScatteringByteChannel channel) throws IOException;
}
