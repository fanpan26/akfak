package com.fanpan26.akfak.common.security.auth;

import com.fanpan26.akfak.common.exceptions.KafkaException;
import com.fanpan26.akfak.common.network.Authenticator;
import com.fanpan26.akfak.common.network.TransportLayer;

import java.security.Principal;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class DefaultPrincipalBuilder implements PrincipalBuilder {

    @Override
    public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
        try {
            return transportLayer.peerPrincipal();
        } catch (Exception e) {
            throw new KafkaException("Failed to build principal due to: ", e);
        }
    }

    @Override
    public void close() throws KafkaException {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
