package com.fanpan26.akfak.common.errors;

import com.fanpan26.akfak.common.KafkaException;

/**
 * @author fanyuepan
 */
public class SerializationException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }

    public SerializationException() {
        super();
    }


    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}