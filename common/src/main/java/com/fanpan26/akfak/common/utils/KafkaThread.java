package com.fanpan26.akfak.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fanyuepan
 */
public class KafkaThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        setDaemon(daemon);
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in " + name + ": ", e);
            }
        });
    }
}
