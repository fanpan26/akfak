package com.fanpan26.akfak.common.record;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author fanyuepan
 */
//TODO
public class MemoryRecords implements Records {
    @Override
    public int sizeInBytes() {
        return 0;
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return null;
    }

    public boolean isFull(){
        return true;
    }

    public void close(){

    }

    public ByteBuffer buffer() {
      return null;
    }
}
