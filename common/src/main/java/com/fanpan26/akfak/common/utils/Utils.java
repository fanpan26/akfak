package com.fanpan26.akfak.common.utils;

import com.fanpan26.akfak.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

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

    public static <T> T notNull(T t) {
        if (t == null) {
            throw new NullPointerException();
        }
        return t;
    }

    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static void closeAll(Closeable... closeables) throws IOException {
        IOException exception = null;
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                }
                else {
                    exception = e;
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

}
