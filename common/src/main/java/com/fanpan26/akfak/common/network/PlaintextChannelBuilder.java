package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.security.auth.PrincipalBuilder;

import java.nio.channels.SelectionKey;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class PlaintextChannelBuilder implements ChannelBuilder {

    private PrincipalBuilder principalBuilder;
    private Map<String, ?> configs;

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            principalBuilder = ChannelBuilders.createPrincipalBuilder(configs);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize) throws KafkaException {
        KafkaChannel channel = null;
        try {
            PlaintextTransportLayer transportLayer = new PlaintextTransportLayer(key);
            Authenticator authenticator = new DefaultAuthenticator();
            authenticator.configure(transportLayer, this.principalBuilder, this.configs);
            channel = new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
        return channel;
    }

    @Override
    public void close() {
        this.principalBuilder.close();
    }
}
