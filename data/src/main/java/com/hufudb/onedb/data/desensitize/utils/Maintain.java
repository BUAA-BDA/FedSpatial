package com.hufudb.onedb.data.desensitize.utils;

import com.hufudb.onedb.data.storage.utils.MethodTypeWrapper;
import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.proto.OneDBData;

public class Maintain extends PojoMethod {

    public Maintain(MethodTypeWrapper type) {
        super(type);
    }

    @Override
    public OneDBData.Method toMethod() {
        return OneDBData.Method.newBuilder().
                setMaintain(OneDBData.Maintain.newBuilder().setType(OneDBData.MethodType.MAINTAIN)).build();
    }
}
