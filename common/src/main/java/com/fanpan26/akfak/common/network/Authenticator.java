package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.exceptions.KafkaException;
import com.fanpan26.akfak.common.security.auth.PrincipalBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;

/**
 * @author fanyuepan
 */
public interface Authenticator extends Closeable {

    void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs);

    void authenticate() throws IOException;

    Principal principal() throws KafkaException;

    boolean complete();

}
