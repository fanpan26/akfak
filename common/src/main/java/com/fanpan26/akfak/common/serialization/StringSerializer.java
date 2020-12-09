package com.fanpan26.akfak.common.serialization;

import com.fanpan26.akfak.common.errors.SerializationException;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class StringSerializer implements Serializer<String> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, String data) {
        try {
            if (data == null) {
                return null;
            }

            return data.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

    @Override
    public void close() {

    }
}
