package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public abstract class AbstractRequestResponse {

    protected final Struct struct;

    public AbstractRequestResponse(Struct struct) {
        this.struct = struct;
    }

    public Struct toStruct() {
        return struct;
    }

    public int sizeOf(){
        return struct.sizeOf();
    }

    public void writeTo(ByteBuffer buffer) {
        struct.writeTo(buffer);
    }

    @Override
    public String toString() {
        return struct.toString();
    }

    @Override
    public int hashCode() {
        return struct.hashCode();
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
        AbstractRequestResponse other = (AbstractRequestResponse) obj;
        return struct.equals(other.struct);
    }
}
