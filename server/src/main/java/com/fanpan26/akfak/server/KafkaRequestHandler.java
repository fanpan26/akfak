package com.fanpan26.akfak.server;

import com.fanpan26.akfak.common.utils.Utils;
import com.fanpan26.akfak.network.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 专门负责处理客户端请求的线程
 * @author fanyuepan
 */
public class KafkaRequestHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private int id;
    private int brokerId;
    private int totalHandlerThreads;
    private RequestChannel requestChannel;
    private KafkaApis apis;

    public KafkaRequestHandler(int id, int brokerId, int threads, RequestChannel requestChannel, KafkaApis apis) {
        this.id = id;
        this.brokerId = brokerId;
        this.totalHandlerThreads = threads;
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    @Override
    public void run() {
        while (true) {
            RequestChannel.Request request = null;
            while (request == null) {
                try {
                    request = requestChannel.receiveRequest(1000);
                    if (request != null) {
                        apis.handle(request);
                    }
                } catch (InterruptedException e) {

                }
            }
        }
    }

    /**
     * 请求处理线程池
     * */
   public static class KafkaRequestHandlerPool {

        private final Thread[] threads;
        private final KafkaRequestHandler[] runnables;

        public KafkaRequestHandlerPool(int brokerId, RequestChannel requestChannel, KafkaApis apis, int numThreads) {
            threads = new Thread[numThreads];
            runnables = new KafkaRequestHandler[numThreads];

            //初始化处理程序，启动线程
            for (int i = 0; i < numThreads; i++) {
                runnables[i] = new KafkaRequestHandler(i, brokerId, numThreads, requestChannel, apis);
                threads[i] = Utils.daemonThread("kafka-request-handler-" + i, runnables[i]);
                threads[i].start();
            }
        }
    }
}
