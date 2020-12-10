package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.protocol.types.Struct;
import com.fanpan26.akfak.common.requests.RequestHeader;

/**
 * @author fanyuepan
 */
//TODO
public class RequestSend extends NetworkSend {

    private final RequestHeader header;
    private final Struct body;

    public RequestSend(String destination,RequestHeader requestHeader,Struct body) {

        this.header = requestHeader;
        this.body = body;
    }
}
