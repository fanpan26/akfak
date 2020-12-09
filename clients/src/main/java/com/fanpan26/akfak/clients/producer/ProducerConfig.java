package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author fanyuepan
 */
public class ProducerConfig  {

    ProducerConfig(Map<?, ?> props) {

    }

    public static Properties addSerializerToConfig(Properties properties,
                                                   Serializer<?> keySerializer, Serializer<?> valueSerializer) {
       return properties;
    }
}
