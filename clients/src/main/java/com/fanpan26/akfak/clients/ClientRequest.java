package com.fanpan26.akfak.clients;

import com.fanpan26.akfak.common.network.RequestSend;

/**
 * @author fanyuepan
 */
public class ClientRequest {

    /**
     * 创建时间
     * */
    private final long createdTimeMs;

    /**
     * 是否期望有返回值
     * */
    private final boolean expectResponse;

    /**
     * 请求包
     * */
    private final RequestSend request;

    /**
     * 回调
     * */
    private final RequestCompletionHandler callback;

    /**
     * 是否由NetworkClient初始化
     * */
    private final boolean isInitiatedByNetworkClient;

    /**
     * 发送时间
     * */
    private long sendTimeMs;


    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback) {
        this(createdTimeMs, expectResponse, request, callback, false);
    }

    public ClientRequest(long createdTimeMs, boolean expectResponse, RequestSend request,
                         RequestCompletionHandler callback, boolean isInitiatedByNetworkClient) {
        this.createdTimeMs = createdTimeMs;
        this.callback = callback;
        this.request = request;
        this.expectResponse = expectResponse;
        this.isInitiatedByNetworkClient = isInitiatedByNetworkClient;
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public RequestSend request() {
        return request;
    }

    public boolean hasCallback() {
        return callback != null;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public boolean isInitiatedByNetworkClient() {
        return isInitiatedByNetworkClient;
    }

    public long sendTimeMs() {
        return sendTimeMs;
    }

    public void setSendTimeMs(long sendTimeMs) {
        this.sendTimeMs = sendTimeMs;
    }
}
