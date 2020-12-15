package com.fanpan26.akfak.common.protocol.types;


import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 可以被序列化的结构体，是Kafka消息的基石
 * @author fanyuepan
 */
public class Struct {

    private final Schema schema;
    private final Object[] values;

    Struct(Schema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public Struct(Schema schema) {
        this.schema = schema;
        this.values = new Object[this.schema.numFields()];
    }

    public Schema schema() {
        return this.schema;
    }

    public Object get(Field field){
        validateField(field);
        return getFieldOrDefault(field);
    }

    public Object get(String name) {
        Field field = schema.get(name);
        if (field == null) {
            throw new SchemaException("No such field: " + name);
        }
        return getFieldOrDefault(field);
    }

    public boolean hasField(String name) {
        return schema.get(name) != null;
    }

    public Struct getStruct(Field field) {
        return (Struct) get(field);
    }

    public Struct getStruct(String name) {
        return (Struct) get(name);
    }

    public Byte getByte(Field field) {
        return (Byte) get(field);
    }

    public byte getByte(String name) {
        return (Byte) get(name);
    }

    public Short getShort(Field field) {
        return (Short) get(field);
    }

    public Short getShort(String name) {
        return (Short) get(name);
    }

    public Integer getInt(Field field) {
        return (Integer) get(field);
    }

    public Integer getInt(String name) {
        return (Integer) get(name);
    }

    public Long getLong(Field field) {
        return (Long) get(field);
    }

    public Long getLong(String name) {
        return (Long) get(name);
    }

    public Object[] getArray(Field field) {
        return (Object[]) get(field);
    }

    public Object[] getArray(String name) {
        return (Object[]) get(name);
    }

    public String getString(Field field) {
        return (String) get(field);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    public Boolean getBoolean(Field field) {
        return (Boolean) get(field);
    }

    public Boolean getBoolean(String name) {
        return (Boolean) get(name);
    }

    public ByteBuffer getBytes(Field field) {
        Object result = get(field);
        if (result instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) result);
        }
        return (ByteBuffer) result;
    }

    public ByteBuffer getBytes(String name) {
        Object result = get(name);
        if (result instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) result);
        }
        return (ByteBuffer) result;
    }

    public Struct set(Field field, Object value) {
        validateField(field);
        this.values[field.index] = value;
        return this;
    }

    public Struct set(String name, Object value) {
        Field field = this.schema.get(name);
        if (field == null) {
            throw new SchemaException("Unknown field: " + name);
        }
        this.values[field.index] = value;
        return this;
    }

    public Struct instance(Field field) {
        validateField(field);
        if (field.type() instanceof Schema) {
            return new Struct((Schema) field.type());
        } else if (field.type() instanceof ArrayOf) {
            ArrayOf array = (ArrayOf) field.type();
            return new Struct((Schema) array.type());
        } else {
            throw new SchemaException("Field '" + field.name + "' is not a container type, it is of type " + field.type());
        }
    }

    public Struct instance(String field) {
        return instance(schema.get(field));
    }

    public void clear() {
        Arrays.fill(this.values, null);
    }

    public int sizeOf() {
        return this.schema.sizeOf(this);
    }

    public void writeTo(ByteBuffer buffer) {
        this.schema.write(buffer, this);
    }

    public void validate() {
        this.schema.validate(this);
    }

    public ByteBuffer[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeOf());
        writeTo(buffer);
        return new ByteBuffer[] {buffer};
    }

    private void validateField(Field field) {
        if (this.schema != field.schema) {
            throw new SchemaException("Attempt to access field '" + field.name + "' from a different schema instance.");
        }
        if (field.index > values.length) {
            throw new SchemaException("Invalid field index: " + field.index);
        }
    }

    private Object getFieldOrDefault(Field field) {
        Object value = this.values[field.index];
        if (value != null) {
            return value;
        }
        if (field.defaultValue != Field.NO_DEFAULT) {
            return field.defaultValue;
        }
        if (field.type.isNullable()) {
            return null;
        }
        throw new SchemaException("Missing value for field '" + field.name + "' which has no default value.");
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            b.append(f.name);
            b.append('=');
            if (f.type() instanceof ArrayOf && this.values[i] != null) {
                Object[] arrayValue = (Object[]) this.values[i];
                b.append('[');
                for (int j = 0; j < arrayValue.length; j++) {
                    b.append(arrayValue[j]);
                    if (j < arrayValue.length - 1) {
                        b.append(',');
                    }
                }
                b.append(']');
            } else {
                b.append(this.values[i]);
            }
            if (i < this.values.length - 1) {
                b.append(',');
            }
        }
        b.append('}');
        return b.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            if (f.type() instanceof ArrayOf) {
                if (this.get(f) != null) {
                    Object[] arrayObject = (Object[]) this.get(f);
                    for (Object arrayItem : arrayObject) {
                        result = prime * result + arrayItem.hashCode();
                    }
                }
            } else {
                Object field = this.get(f);
                if (field != null) {
                    result = prime * result + field.hashCode();
                }
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Struct other = (Struct) obj;
        if (schema != other.schema) {
            return false;
        }
        for (int i = 0; i < this.values.length; i++) {
            Field f = this.schema.get(i);
            boolean result;
            if (f.type() instanceof ArrayOf) {
                result = Arrays.equals((Object[]) this.get(f), (Object[]) other.get(f));
            } else {
                Object thisField = this.get(f);
                Object otherField = other.get(f);
                result = (thisField == null && otherField == null) || thisField.equals(otherField);
            }
            if (!result) {
                return false;
            }
        }
        return true;
    }

}
