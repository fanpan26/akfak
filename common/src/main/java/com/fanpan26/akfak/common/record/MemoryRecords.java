package com.fanpan26.akfak.common.record;

import java.util.Iterator;

/**
 * @author fanyuepan
 */
public class MemoryRecords implements Records {
    @Override
    public int sizeInBytes() {
        return 0;
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return null;
    }
}
