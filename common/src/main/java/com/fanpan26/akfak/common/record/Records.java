package com.fanpan26.akfak.common.record;

/**
 * @author fanyuepan
 */
public interface Records extends Iterable<LogEntry> {

    int SIZE_LENGTH = 4;
    int OFFSET_LENGTH = 8;
    int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;

    /**
     * The size of these records in bytes
     */
    int sizeInBytes();
}