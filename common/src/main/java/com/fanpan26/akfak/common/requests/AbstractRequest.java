package com.fanpan26.akfak.common.requests;

import com.fanpan26.akfak.common.protocol.types.Struct;

/**
 * @author fanyuepan
 */
public abstract class AbstractRequest extends AbstractRequestResponse {
    public AbstractRequest(Struct struct) {
        super(struct);
    }
}
