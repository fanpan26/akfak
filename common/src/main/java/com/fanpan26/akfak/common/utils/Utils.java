package com.fanpan26.akfak.common.utils;

import com.fanpan26.akfak.common.exceptions.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fanyuepan
 */
public final class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static <T> T newInstance(Class<T> c) {
        try {
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new KafkaException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        } catch (NullPointerException e) {
            throw new KafkaException("Requested class was null", e);
        }
    }

    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                // IPv6
                ? "[" + host + "]:" + port
                : host + ":" + port;
    }

    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }
}
