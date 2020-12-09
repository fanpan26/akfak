package com.fanpan26.akfak.cluster;

import com.fanpan26.akfak.common.network.SecurityProtocol;
import com.fanpan26.akfak.common.utils.Utils;

/**
 * @author fanyuepan
 */
public class EndPoint {

    private String host;
    private int port;
    private SecurityProtocol protocol;

    public EndPoint(String host, int port, SecurityProtocol protocol) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
    }

    public String connectionString() {
        String hostPort = host == null ? ":" + port : Utils.formatAddress(host, port);
        return protocol + "://" + hostPort;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public SecurityProtocol getProtocol() {
        return protocol;
    }
}
