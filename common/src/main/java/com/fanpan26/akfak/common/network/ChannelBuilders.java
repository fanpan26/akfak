package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.security.auth.DefaultPrincipalBuilder;
import com.fanpan26.akfak.common.security.auth.PrincipalBuilder;
import com.fanpan26.akfak.common.utils.Utils;

import java.util.Map;

/**
 * @author fanyuepan
 */
public class ChannelBuilders {

    private ChannelBuilders(){

    }

    public static ChannelBuilder create(SecurityProtocol protocol,
                                        Mode mode,
                                        LoginType loginType,
                                        Map<String,?> configs,
                                        String clientSaslMechanism,
                                        boolean saslHandshakeRequestEnable) {
        ChannelBuilder channelBuilder = null;

        switch (protocol){
            case SSL:
                break;
            case SASL_SSL:
            case SASL_PLAINTEXT:
                break;
            case TRACE:
            case PLAINTEXT:
                channelBuilder = new PlaintextChannelBuilder();
                break;
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

    public static PrincipalBuilder createPrincipalBuilder(Map<String,?> configs) {
        Class<?> principalBuilderClass = (Class<?>) configs.get("principal.builder.class");
        PrincipalBuilder principalBuilder;
        if (principalBuilderClass == null) {
            principalBuilder = new DefaultPrincipalBuilder();
        } else {
            principalBuilder = (PrincipalBuilder) Utils.newInstance(principalBuilderClass);
        }
        principalBuilder.configure(configs);
        return principalBuilder;
    }
}
