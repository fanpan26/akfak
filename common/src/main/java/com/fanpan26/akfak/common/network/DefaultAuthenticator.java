package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.security.auth.PrincipalBuilder;

import java.io.IOException;
import java.security.Principal;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class DefaultAuthenticator implements Authenticator {

    private TransportLayer transportLayer;
    private PrincipalBuilder principalBuilder;
    private Principal principal;

    @Override
    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) {
        this.transportLayer = transportLayer;
        this.principalBuilder = principalBuilder;
    }


    @Override
    public void authenticate() throws IOException {

    }

    @Override
    public Principal principal() throws KafkaException {
        if (principal == null) {
            principal = principalBuilder.buildPrincipal(transportLayer, this);
        }
        return principal;
    }

    @Override
    public boolean complete() {
        return true;
    }

    @Override
    public void close() throws IOException {

    }
}
