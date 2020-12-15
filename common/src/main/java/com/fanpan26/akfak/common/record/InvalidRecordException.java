package com.fanpan26.akfak.common.record;

import com.fanpan26.akfak.common.KafkaException;

/**
 * @author fanyuepan
 */
public class InvalidRecordException extends KafkaException {

    private static final long serialVersionUID = 1;

    public InvalidRecordException(String s) {
        super(s);
    }

}

