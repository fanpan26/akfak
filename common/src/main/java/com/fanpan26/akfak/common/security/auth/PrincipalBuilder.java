package com.fanpan26.akfak.common.security.auth;

import com.fanpan26.akfak.common.Configurable;
import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.network.Authenticator;
import com.fanpan26.akfak.common.network.TransportLayer;

import java.security.Principal;

/**
 * @author fanyuepan
 */
public interface PrincipalBuilder extends Configurable {

    Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException;

    void close() throws KafkaException;
}
