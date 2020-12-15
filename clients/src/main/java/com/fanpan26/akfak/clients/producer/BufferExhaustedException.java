package com.fanpan26.akfak.clients.producer;

import com.fanpan26.akfak.common.KafkaException;

/**
 * @author fanyuepan
 */
public class BufferExhaustedException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }

}
