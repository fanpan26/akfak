package com.fanpan26.akfak.common.protocol.types;

import com.fanpan26.akfak.common.KafkaException;

/**
 * @author fanyuepan
 */
public class SchemaException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }
}
