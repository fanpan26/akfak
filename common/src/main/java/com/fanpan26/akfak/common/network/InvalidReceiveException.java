package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.KafkaException;

/**
 * @author fanyuepan
 */
public class InvalidReceiveException extends KafkaException {

    public InvalidReceiveException(String message) {
        super(message);
    }

    public InvalidReceiveException(String message, Throwable cause) {
        super(message, cause);
    }
}
