package com.fanpan26.akfak.network;

import com.fanpan26.akfak.common.network.KafkaChannel;
import com.fanpan26.akfak.common.network.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author fanyuepan
 */
public abstract class AbstractServerThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractServerThread.class);

    private CountDownLatch startupLatch = new CountDownLatch(1);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean alive = new AtomicBoolean(true);

    abstract void wakeup();

    void shutdown() {
        alive.set(false);
        wakeup();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error("shutdown interrupted", e);
        }
    }

    void awaitStartup() {
        try {
            startupLatch.await();
        } catch (InterruptedException e) {
            logger.error("startup interrupted", e);
        }
    }

    protected void startupComplete() {
        startupLatch.countDown();
    }

    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    protected boolean isRunning() {
        return alive.get();
    }

    void close(Selector selector, String source) {
        KafkaChannel channel = selector.channel(source);
        if (channel != null) {
            selector.close(source);
        }
    }

    void close(SocketChannel channel) {
        if (channel != null) {
            try {
                channel.socket().close();
                channel.close();
            } catch (Exception e) {
                logger.error("close channel error", e);
            }
        }
    }
}
