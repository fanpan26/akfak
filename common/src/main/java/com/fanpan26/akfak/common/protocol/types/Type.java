package com.fanpan26.akfak.common.protocol.types;

import com.fanpan26.akfak.common.utils.Utils;

import java.nio.ByteBuffer;

/**
 * 可以序列化的类型
 * @author fanyuepan
 */
public abstract class Type {

    /**
     * 将一个object写入ByteBuffer
     */
    public abstract void write(ByteBuffer buffer, Object o);

    /**
     * 从ByteBuffer中读出object
     */
    public abstract Object read(ByteBuffer buffer);

    /**
     * 校验
     */
    public abstract Object validate(Object o);

    /**
     * Object 的 byte size
     */
    public abstract int sizeOf(Object o);

    /**
     * 是否支持NullValue
     */
    public boolean isNullable() {
        return false;
    }

    /**
     * 基础类型 Boolean
     */
    public static final Type BOOLEAN = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            if ((Boolean) o) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
        }

        @Override
        public Object read(ByteBuffer buffer) {
            byte b = buffer.get();
            return b != 0;
        }

        @Override
        public Object validate(Object o) {
            if (o instanceof Boolean) {
                return o;
            }
            throw new SchemaException(o + " is not a Boolean.");
        }

        @Override
        public int sizeOf(Object o) {
            return 1;
        }

        @Override
        public String toString() {
            return "BOOLEAN";
        }
    };

    public static final Type INT8 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.put((Byte) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.get();
        }

        @Override
        public Object validate(Object o) {
            if (o instanceof Byte) {
                return o;
            }
            throw new SchemaException(o + " is not a Byte.");
        }

        @Override
        public int sizeOf(Object o) {
            return 1;
        }

        @Override
        public String toString() {
            return "INT8";
        }
    };

    public static final Type INT16 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putShort((Short) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getShort();
        }

        @Override
        public int sizeOf(Object o) {
            return 2;
        }

        @Override
        public String toString() {
            return "INT16";
        }

        @Override
        public Short validate(Object item) {
            if (item instanceof Short) {
                return (Short) item;
            }

            throw new SchemaException(item + " is not a Short.");
        }
    };

    public static final Type INT32 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putInt((Integer) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getInt();
        }

        @Override
        public int sizeOf(Object o) {
            return 4;
        }

        @Override
        public String toString() {
            return "INT32";
        }

        @Override
        public Integer validate(Object item) {
            if (item instanceof Integer) {
                return (Integer) item;
            }
            throw new SchemaException(item + " is not an Integer.");
        }
    };

    public static final Type INT64 = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            buffer.putLong((Long) o);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            return buffer.getLong();
        }

        @Override
        public int sizeOf(Object o) {
            return 8;
        }

        @Override
        public String toString() {
            return "INT64";
        }

        @Override
        public Long validate(Object item) {
            if (item instanceof Long) {
                return (Long) item;
            }

            throw new SchemaException(item + " is not a Long.");
        }
    };

    public static final Type STRING = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            byte[] bytes = Utils.utf8((String) o);
            //String 长度限制 32767
            if (bytes.length > Short.MAX_VALUE) {
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            }
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            //编码不对
            if (length < 0) {
                throw new SchemaException("String length " + length + " cannot be negative");
            }
            //长度与真实字符串bytes长度不对应
            if (length > buffer.remaining()) {
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
            return "STRING";
        }

        @Override
        public String validate(Object item) {
            if (item instanceof String) {
                return (String) item;
            }

            throw new SchemaException(item + " is not a String.");
        }
    };

    /**
     * 可空字符串
     */
    public static final Type NULLABLE_STRING = new Type() {

        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            //如果字符串为空，那么编码 -1
            if (o == null) {
                buffer.putShort((short) -1);
                return;
            }

            byte[] bytes = Utils.utf8((String) o);
            if (bytes.length > Short.MAX_VALUE) {
                throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
            }
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            short length = buffer.getShort();
            if (length < 0) {
                return null;
            }
            if (length > buffer.remaining()) {
                throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");
            }
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return Utils.utf8(bytes);
        }

        @Override
        public int sizeOf(Object o) {
            //null的话，长度为short类型的-1 占用2个字节
            if (o == null) {
                return 2;
            }

            return 2 + Utils.utf8Length((String) o);
        }

        @Override
        public String toString() {
            return "NULLABLE_STRING";
        }

        @Override
        public String validate(Object item) {
            if (item == null) {
                return null;
            }

            if (item instanceof String) {
                return (String) item;
            }

            throw new SchemaException(item + " is not a String.");
        }
    };

    public static final Type BYTES = new Type() {
        @Override
        public void write(ByteBuffer buffer, Object o) {
            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            //读出buffer的大小放入
            buffer.putInt(arg.remaining());
            //在将buffer放入
            buffer.put(arg);
            //重置位置
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            //不可能小于0
            if (size < 0) {
                throw new SchemaException("Bytes size " + size + " cannot be negative");
            }
            if (size > buffer.remaining()) {
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");
            }
            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            ByteBuffer buffer = (ByteBuffer) o;
            //4字节的长度 + bytes真实长度
            return 4 + buffer.remaining();
        }

        @Override
        public String toString() {
            return "BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item instanceof ByteBuffer) {
                return (ByteBuffer) item;
            } else {
                throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
            }
        }
    };

    public static final Type NULLABLE_BYTES = new Type() {
        @Override
        public boolean isNullable() {
            return true;
        }

        @Override
        public void write(ByteBuffer buffer, Object o) {
            if (o == null) {
                buffer.putInt(-1);
                return;
            }

            ByteBuffer arg = (ByteBuffer) o;
            int pos = arg.position();
            buffer.putInt(arg.remaining());
            buffer.put(arg);
            arg.position(pos);
        }

        @Override
        public Object read(ByteBuffer buffer) {
            int size = buffer.getInt();
            if (size < 0) {
                return null;
            }
            if (size > buffer.remaining()) {
                throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + " bytes available");
            }
            ByteBuffer val = buffer.slice();
            val.limit(size);
            buffer.position(buffer.position() + size);
            return val;
        }

        @Override
        public int sizeOf(Object o) {
            if (o == null) {
                //int类型的-1
                return 4;
            }

            ByteBuffer buffer = (ByteBuffer) o;
            return 4 + buffer.remaining();
        }

        @Override
        public String toString() {
            return "NULLABLE_BYTES";
        }

        @Override
        public ByteBuffer validate(Object item) {
            if (item == null) {
                return null;
            }

            if (item instanceof ByteBuffer) {
                return (ByteBuffer) item;
            }

            throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
        }
    };
}
