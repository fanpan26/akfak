package com.fanpan26.akfak.network;

import com.fanpan26.akfak.common.requests.AbstractRequest;
import com.fanpan26.akfak.common.requests.RequestHeader;
import com.fanpan26.akfak.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author fanyuepan
 */
public class RequestChannel {

    private static final Logger logger = LoggerFactory.getLogger(RequestChannel.class);

    private final BlockingQueue<Request> requestQueue;

    private final BlockingQueue<Response>[] responseQueue;

    public RequestChannel(int numProcessors, int maxRequestSize) {
        this.requestQueue = new ArrayBlockingQueue<>(maxRequestSize);
        responseQueue = new BlockingQueue[numProcessors];
        for (int i = 0; i < numProcessors; i++) {
            responseQueue[i] = new LinkedBlockingQueue<>();
        }
    }

    /**
     * 将Request放进待处理的队列中
     */
    public void sendRequest(Request request) throws InterruptedException {
        logger.info("将已经收到的请求{}放入队列", request);
        requestQueue.put(request);
    }

    /**
     * 将响应放进响应队列
     * */
    public void sendResponse(Response response) throws InterruptedException {
        logger.info("将已经处理的的请求响应{}放入队列", response);
        responseQueue[response.processor].put(response);
    }

    public Response receiveResponse(int processor){
        return responseQueue[processor].poll();
    }

    /**
     * 取出Request
     */
    public Request receiveRequest(long timeout) throws InterruptedException {
        return requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public static class Request {

        public final int processor;
        public final String connectionId;
        public final RequestHeader header;
        public final AbstractRequest body;

        public Request(int processorId, String connectionId, ByteBuffer buffer) {
            this.processor = processorId;
            this.connectionId = connectionId;
            this.header = RequestHeader.parse(buffer);
            this.body = AbstractRequest.getRequest(this.header.apiKey(), header.apiVersion(), buffer);
        }

        @Override
        public String toString() {
            return this.header.toString() + this.body.toString();
        }
    }

    public static class Response {
        public final int processor;
        public final Request request;
        public final ResponseSend responseSend;

        public Response(int processor, Request request, ResponseSend responseSend) {
            this.processor = processor;
            this.request = request;
            this.responseSend = responseSend;
        }

        @Override
        public String toString() {
            return "processor:" + processor + "," + responseSend.toString();
        }
    }
}
