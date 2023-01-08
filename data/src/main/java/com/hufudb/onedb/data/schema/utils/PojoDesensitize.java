package com.hufudb.onedb.data.schema.utils;

import com.hufudb.onedb.data.storage.utils.SensitivityWrapper;
import com.hufudb.onedb.proto.OneDBData;

public class PojoDesensitize {
    public SensitivityWrapper sensitivity;
    public PojoMethod method;

    public PojoDesensitize() {}

    public PojoDesensitize(SensitivityWrapper sensitivity, PojoMethod method) {
        this.sensitivity = sensitivity;
        this.method = method;
    }

    public OneDBData.Sensitivity getSensitivity() {
        return sensitivity.get();
    }

    public static PojoDesensitize fromDesensitize(OneDBData.Desensitize desensitize) {
        return new PojoDesensitize(SensitivityWrapper.of(desensitize.getSensitivity()), PojoMethod.fromColumnMethod(desensitize.getMethod()));
    }

    public OneDBData.Desensitize toDesensitize() {
        method  = method == null ? PojoMethod.methodDefault() : method;
        return OneDBData.Desensitize.newBuilder().setSensitivity(getSensitivity()).setMethod(method.toMethod()).build();
    }

    public static PojoDesensitize desensitizeDefault() {
        return PojoDesensitize.fromDesensitize(OneDBData.Desensitize.newBuilder().setSensitivity(OneDBData.Sensitivity.PLAIN).setMethod(OneDBData.Method.newBuilder().setMaintain(OneDBData.Maintain.newBuilder().setType(OneDBData.MethodType.MAINTAIN))).build());
    }
}
