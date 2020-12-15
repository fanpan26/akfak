package com.fanpan26.akfak.common.record;

import com.fanpan26.akfak.common.KafkaException;
import com.fanpan26.akfak.common.utils.AbstractIterator;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * @author fanyuepan
 */
public class MemoryRecords implements Records {

    private final static int WRITE_LIMIT_FOR_READABLE_ONLY = -1;

    private final Compressor compressor;

    private final int writeLimit;

    private final int initialCapacity;

    private ByteBuffer buffer;

    private boolean writable;

    private MemoryRecords(ByteBuffer buffer,CompressionType type,boolean writeable,int writeLimit){
        this.writable = writeable;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int writeLimit) {
        return new MemoryRecords(buffer, type, true, writeLimit);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        // use the buffer capacity as the default write limit
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, WRITE_LIMIT_FOR_READABLE_ONLY);
    }

    public void append(long offset,Record record){
        if (!writable){
            throw new IllegalStateException("Memory records is not writable");
        }
        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    public long append(long offset,long timestamp,byte[] key,byte[] value) {
        if (!writable) {
            throw new IllegalStateException("Memory records is not writable");
        }
        int size = Record.recordSize(key, value);
        compressor.putLong(offset);
        compressor.putInt(size);
        long crc = compressor.putRecord(timestamp, key, value);
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        return crc;
    }



    @Override
    public int sizeInBytes() {
        if (writable) {
            return compressor.buffer().position();
        } else {
            return buffer.limit();
        }
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (writable) {
            // flip on a duplicate buffer for reading
            return new RecordsIterator((ByteBuffer) this.buffer.duplicate().flip(), false);
        } else {
            // do not need to flip for non-writable buffer
            return new RecordsIterator(this.buffer.duplicate(), false);
        }
    }
    public boolean isFull(){
        return !this.writable || this.writeLimit <= this.compressor.estimatedBytesWritten();
    }

    public double compressionRate() {
        if (compressor == null) {
            return 1.0;
        }
        return compressor.compressionRate();
    }

    public void close(){
        if (writable){
            compressor.close();
            buffer = compressor.buffer();
            buffer.flip();
            writable = false;
        }
    }

    public boolean hasRoomFor(byte[] key,byte[] value){
        if (!this.writable){
            return false;
        }
        return this.compressor.numRecordsWritten() == 0 ?
                this.initialCapacity >= Records.LOG_OVERHEAD + Record.recordSize(key, value) :
                this.writeLimit >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value);
    }

    public int initialCapacity() {
        return this.initialCapacity;
    }

    /**
     * Get the byte buffer that backs this records instance for reading
     */
    public ByteBuffer buffer() {
        if (writable) {
            throw new IllegalStateException("The memory records must not be writable any more before getting its underlying buffer");
        }
        return buffer.duplicate();
    }

    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    public boolean isWritable() {
        return writable;
    }

    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;
        private final DataInputStream stream;
        private final CompressionType type;
        private final boolean shallow;
        private RecordsIterator innerIter;

        // The variables for inner iterator
        private final ArrayDeque<LogEntry> logEntries;
        private final long absoluteBaseOffset;

        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            this.type = CompressionType.NONE;
            this.buffer = buffer;
            this.shallow = shallow;
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        // Private constructor for inner iterator.
        private RecordsIterator(LogEntry entry) {
            this.type = entry.record().compressionType();
            this.buffer = entry.record().value();
            this.shallow = true;
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, entry.record().magic());
            long wrapperRecordOffset = entry.offset();
            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset.
            if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                this.logEntries = new ArrayDeque<>();
                long wrapperRecordTimestamp = entry.record().timestamp();
                while (true) {
                    try {
                        LogEntry logEntry = getNextEntryFromStream();
                        Record recordWithTimestamp = new Record(logEntry.record().buffer(),
                                wrapperRecordTimestamp,
                                entry.record().timestampType());
                        logEntries.add(new LogEntry(logEntry.offset(), recordWithTimestamp));
                    } catch (EOFException e) {
                        break;
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
                this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
            } else {
                this.logEntries = null;
                this.absoluteBaseOffset = -1;
            }

        }

        /*
         * Read the next record from the buffer.
         *
         * Note that in the compressed message set, each message value size is set as the size of the un-compressed
         * version of the message value, so when we do de-compression allocating an array of the specified size for
         * reading compressed value data is sufficient.
         */
        @Override
        protected LogEntry makeNext() {
            if (innerDone()) {
                try {
                    LogEntry entry = getNextEntry();
                    // No more record to return.
                    if (entry == null)
                        return allDone();

                    // Convert offset to absolute offset if needed.
                    if (absoluteBaseOffset >= 0) {
                        long absoluteOffset = absoluteBaseOffset + entry.offset();
                        entry = new LogEntry(absoluteOffset, entry.record());
                    }

                    // decide whether to go shallow or deep iteration if it is compressed
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        return entry;
                    } else {
                        // init the inner iterator with the value payload of the message,
                        // which will de-compress the payload to a set of messages;
                        // since we assume nested compression is not allowed, the deep iterator
                        // would not try to further decompress underlying messages
                        // There will be at least one element in the inner iterator, so we don't
                        // need to call hasNext() here.
                        innerIter = new RecordsIterator(entry);
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private LogEntry getNextEntry() throws IOException {
            if (logEntries != null) {
                return getNextEntryFromEntryList();
            }

            return getNextEntryFromStream();
        }

        private LogEntry getNextEntryFromEntryList() {
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        private LogEntry getNextEntryFromStream() throws IOException {
            // read the offset
            long offset = stream.readLong();
            // read record size
            int size = stream.readInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);
            // read the record, if compression is used we cannot depend on size
            // and hence has to do extra copy
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                rec = buffer.slice();
                int newPos = buffer.position() + size;
                if (newPos > buffer.limit())
                    return null;
                buffer.position(newPos);
                rec.limit(size);
            } else {
                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                rec = ByteBuffer.wrap(recordBuffer);
            }
            return new LogEntry(offset, new Record(rec));
        }

        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
    }
}
