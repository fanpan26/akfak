package com.fanpan26.akfak.common.protocol.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanyuepan
 */
public class Schema extends Type {

    private final Field[] fields;
    private final Map<String,Field> fieldsByName;


    /**
     * 一个Schema 存放多个Field
     * */
    public Schema(Field... fs) {
        this.fields = new Field[fs.length];
        this.fieldsByName = new HashMap<>();
        for (int i = 0; i < this.fields.length; i++) {
            Field field = fs[i];
            if (fieldsByName.containsKey(field.name)) {
                throw new SchemaException("Schema contains a duplicate field: " + field.name);
            }
            this.fields[i] = new Field(i, field.name, field.type, field.doc, field.defaultValue, this);
            this.fieldsByName.put(fs[i].name, this.fields[i]);
        }
    }

    public Field[] fields() {
        return fields;
    }

    public Field get(String name){
        return this.fieldsByName.get(name);
    }

    /**
     * Write a struct to the buffer
     * */
    @Override
    public void write(ByteBuffer buffer, Object o) {
        Struct r = (Struct) o;
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            try {
                Object value = f.type().validate(r.get(f));
                f.type.write(buffer, value);
            } catch (Exception e) {
                throw new SchemaException("Error writing field '" + f.name +
                        "': " +
                        (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
    }

    @Override
    public Object validate(Object o) {
        try {
            Struct struct = (Struct) o;
            for (int i = 0; i < this.fields.length; i++) {
                Field field = this.fields[i];
                try {
                    field.type.validate(struct.get(field));
                } catch (SchemaException e) {
                    throw new SchemaException("Invalid value for field '" + field.name + "': " + e.getMessage());
                }
            }
            return struct;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a Struct.");
        }
    }

    @Override
    public int sizeOf(Object o) {
        int size = 0;
        Struct r = (Struct)o;
        for (int i = 0;i<fields.length;i++){
            size += fields[i].type.sizeOf(r.get(fields[i]));
        }
        return size;
    }

    public int numFields() {
        return this.fields.length;
    }

    public Field get(int slot) {
        return this.fields[slot];
    }

    @Override
    public Struct read(ByteBuffer buffer) {
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            try {
                objects[i] = fields[i].type.read(buffer);
            } catch (Exception e) {
                throw new SchemaException("Error reading field '" + fields[i].name +
                        "': " +
                        (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
        return new Struct(this, objects);
    }
}
