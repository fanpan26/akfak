package com.fanpan26.akfak.common.record;

import com.fanpan26.akfak.common.utils.Crc32;
import com.fanpan26.akfak.common.utils.Utils;

import java.nio.ByteBuffer;

/**
 * 一条消息记录
 * @author fanyuepan
 */
public final class Record {

    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;

    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;

    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTE_LENGTH = 1;

    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;

    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;

    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;

    public static final int VALUE_SIZE_LENGTH = 4;

    public static final int HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH;

    public static final int RECORD_OVERHEAD = HEADER_SIZE + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    public static final byte MAGIC_VALUE_V0 = 0;
    public static final byte MAGIC_VALUE_V1 = 1;

    public static final byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V1;

    public static final int COMPRESSION_CODEC_MASK = 0x07;

    public static final byte TIMESTAMP_TYPE_MASK = 0x08;
    public static final int TIMESTAMP_TYPE_ATTRIBUTE_OFFSET = 3;

    public static final int NO_COMPRESSION = 0;

    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public Record(ByteBuffer buffer) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = null;
        this.wrapperRecordTimestampType = null;
    }

    Record(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    public Record(long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        this(ByteBuffer.allocate(recordSize(key == null ? 0 : key.length,
                value == null ? 0 : valueSize >= 0 ? valueSize : value.length - valueOffset)));
        write(this.buffer, timestamp, key, value, type, valueOffset, valueSize);
        this.buffer.rewind();
    }

    public Record(long timestamp, byte[] key, byte[] value, CompressionType type) {
        this(timestamp, key, value, type, 0, -1);
    }

    public Record(long timestamp, byte[] value, CompressionType type) {
        this(timestamp, null, value, type);
    }

    public Record(long timestamp, byte[] key, byte[] value) {
        this(timestamp, key, value, CompressionType.NONE);
    }

    public Record(long timestamp, byte[] value) {
        this(timestamp, null, value, CompressionType.NONE);
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    public static void write(ByteBuffer buffer, long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        // construct the compressor with compression type none since this function will not do any
        //compression according to the input type, it will just write the record's payload as is
        Compressor compressor = new Compressor(buffer, CompressionType.NONE);
        try {
            compressor.putRecord(timestamp, key, value, type, valueOffset, valueSize);
        } finally {
            compressor.close();
        }
    }

    public static void write(Compressor compressor, long crc, byte attributes, long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // write crc
        compressor.putInt((int) (crc & 0xffffffffL));
        // write magic value
        compressor.putByte(CURRENT_MAGIC_VALUE);
        // write attributes
        compressor.putByte(attributes);
        // write timestamp
        compressor.putLong(timestamp);
        // write the key
        if (key == null) {
            compressor.putInt(-1);
        } else {
            compressor.putInt(key.length);
            compressor.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            compressor.putInt(size);
            compressor.put(value, valueOffset, size);
        }
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(int keySize, int valueSize) {
        return CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    public static byte computeAttributes(CompressionType type) {
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        return attributes;
    }

    /**
     * Compute the checksum of the record from the record contents
     */
    public static long computeChecksum(ByteBuffer buffer, int position, int size) {
        Crc32 crc = new Crc32();
        crc.update(buffer.array(), buffer.arrayOffset() + position, size);
        return crc.getValue();
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    public static long computeChecksum(long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        Crc32 crc = new Crc32();
        crc.update(CURRENT_MAGIC_VALUE);
        byte attributes = 0;
        if (type.id > 0) {
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        }
        crc.update(attributes);
        crc.updateLong(timestamp);
        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(key.length);
            crc.update(key, 0, key.length);
        }
        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            crc.updateInt(size);
            crc.update(value, valueOffset, size);
        }
        return crc.getValue();
    }


    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this record
     */
    public long checksum() {
        return Utils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the record matches the crc computed off the record contents
     */
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidRecordException if isValid is false for this record
     */
    public void ensureValid() {
        if (!isValid()) {
            throw new InvalidRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = "
                    + computeChecksum()
                    + ")");
        }
    }

    /**
     * The complete serialized size of this record in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        if (magic() == MAGIC_VALUE_V0) {
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        }

        return buffer.getInt(KEY_SIZE_OFFSET_V1);
    }

    /**
     * Does the record have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the value size is stored
     */
    private int valueSizeOffset() {
        if (magic() == MAGIC_VALUE_V0) {
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        }

        return KEY_OFFSET_V1 + Math.max(0, keySize());
    }

    /**
     * The length of the value in bytes
     */
    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    /**
     * The magic version of this record
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this record
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * When magic value is greater than 0, the timestamp of a record is determined in the following way:
     * 1. wrapperRecordTimestampType = null and wrapperRecordTimestamp is null - Uncompressed message, timestamp is in the message.
     * 2. wrapperRecordTimestampType = LOG_APPEND_TIME and WrapperRecordTimestamp is not null - Compressed message using LOG_APPEND_TIME
     * 3. wrapperRecordTimestampType = CREATE_TIME and wrapperRecordTimestamp is not null - Compressed message using CREATE_TIME
     */
    public long timestamp() {
        if (magic() == MAGIC_VALUE_V0) {
            return NO_TIMESTAMP;
        } else {
            // case 2
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null) {
                return wrapperRecordTimestamp;
            }
            // Case 1, 3
            return buffer.getLong(TIMESTAMP_OFFSET);
        }
    }

    /**
     * The timestamp of the message.
     */
    public TimestampType timestampType() {
        if (magic() == 0) {
            return TimestampType.NO_TIMESTAMP_TYPE;
        }
        return wrapperRecordTimestampType == null ? TimestampType.forAttributes(attributes()) : wrapperRecordTimestampType;
    }

    /**
     * The compression type used with this record
     */
    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the value of this record
     */
    public ByteBuffer value() {
        return sliceDelimited(valueSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        if (magic() == MAGIC_VALUE_V0) {
            return sliceDelimited(KEY_SIZE_OFFSET_V0);
        }
        return sliceDelimited(KEY_SIZE_OFFSET_V1);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    @Override
    public String toString() {
        if (magic() > 0) {
            return String.format("Record(magic = %d, attributes = %d, compression = %s, crc = %d, %s = %d, key = %d bytes, value = %d bytes)",
                    magic(),
                    attributes(),
                    compressionType(),
                    checksum(),
                    timestampType(),
                    timestamp(),
                    key() == null ? 0 : key().limit(),
                    value() == null ? 0 : value().limit());
        }

        return String.format("Record(magic = %d, attributes = %d, compression = %s, crc = %d, key = %d bytes, value = %d bytes)",
                magic(),
                attributes(),
                compressionType(),
                checksum(),
                key() == null ? 0 : key().limit(),
                value() == null ? 0 : value().limit());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!other.getClass().equals(Record.class)) {
            return false;
        }
        Record record = (Record) other;
        return this.buffer.equals(record.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

}
