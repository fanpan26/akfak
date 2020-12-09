package com.fanpan26.akfak.common.record;

/**
 * @author fanyuepan
 */
public final class LogEntry {
    private final long offset;
    private final Record record;

    public LogEntry(long offset, Record record) {
        this.offset = offset;
        this.record = record;
    }

    public long offset() {
        return this.offset;
    }

    public Record record() {
        return this.record;
    }

    @Override
    public String toString() {
        return "LogEntry(" + offset + ", " + record + ")";
    }

    public int size() {
        return record.size() + Records.LOG_OVERHEAD;
    }
}
