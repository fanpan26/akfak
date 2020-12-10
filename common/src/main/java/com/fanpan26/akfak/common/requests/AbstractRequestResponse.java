package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.protocol.types.Struct;

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
}
