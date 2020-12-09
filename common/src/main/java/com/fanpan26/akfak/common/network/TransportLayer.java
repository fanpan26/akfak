package com.fanpan26.akfak.common.network;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;
import java.security.Principal;

/**
 * @author fanyuepan
 */
public interface TransportLayer extends ScatteringByteChannel,GatheringByteChannel {

    boolean ready();

    boolean finishConnect() throws IOException;

    void disconnect();

    boolean isConnected();

    SocketChannel socketChannel();

    void handshake();

    boolean hasPendingWrites();

    Principal peerPrincipal() throws IOException;

    void addInterestOps(int ops);

    void removeInterestOps(int ops);

    boolean isMute();

    long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;
}
