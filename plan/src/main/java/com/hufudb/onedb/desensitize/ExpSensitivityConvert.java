package com.hufudb.onedb.desensitize;

import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.ExpSensitivity;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExpSensitivityConvert {
    private static final List<Integer> list1 = Arrays.asList(0, 0, 2);
    private static final List<Integer> list2 = Arrays.asList(0, 1, 1);
    private static final List<Integer> list3 = Arrays.asList(0, 2, 0);
    private static final List<Integer> list4 = Arrays.asList(1, 0, 1);
    private static final List<Integer> list5 = Arrays.asList(1, 1, 0);
    private static final List<Integer> list6 = Arrays.asList(2, 0, 0);

    public static ExpSensitivity convertBinary(Map<ExpSensitivity, Integer> map) {
        Integer plain = map.get(ExpSensitivity.NONE_SENSITIVE) == null ? 0 : map.get(ExpSensitivity.NONE_SENSITIVE);
        Integer singleSensitive = map.get(ExpSensitivity.SINGLE_SENSITIVE) == null ? 0 : map.get(ExpSensitivity.SINGLE_SENSITIVE);
        Integer multiSensitive = map.get(ExpSensitivity.MULTI_SENSITIVE) == null ? 0 : map.get(ExpSensitivity.MULTI_SENSITIVE);
        List<Integer> list = Arrays.asList(plain, singleSensitive, multiSensitive);
        if (list.equals(list1)) {
            return ExpSensitivity.MULTI_SENSITIVE;
        }
        if (list.equals(list2)) {
            return ExpSensitivity.MULTI_SENSITIVE;
        }
        if (list.equals(list3)) {
            return ExpSensitivity.MULTI_SENSITIVE;
        }
        if (list.equals(list4)) {
            return ExpSensitivity.MULTI_SENSITIVE;
        }
        if (list.equals(list5)) {
            return ExpSensitivity.ERROR;
        }
        if (list.equals(list6)) {
            return ExpSensitivity.NONE_SENSITIVE;
        }
        return ExpSensitivity.ERROR;
    }

    public static ExpSensitivity convertAggFunctions(ExpSensitivity tmp, int funId) {
        if (AggFuncType.of(funId) == AggFuncType.GROUPKEY) {
            return tmp;
        }
        if (AggFuncType.of(funId) == AggFuncType.COUNT) {
            return ExpSensitivity.NONE_SENSITIVE;
        } else {
            if (tmp == ExpSensitivity.SINGLE_SENSITIVE) {
                return ExpSensitivity.ERROR;
            } else {
                return tmp;
            }
        }
    }

    public static Expression toProto(Expression exp, ExpSensitivity expSensitivity) {
        Expression.Builder builder = Expression.newBuilder();
        builder.setOpType(exp.getOpType());
        builder.setOutType(exp.getOutType());
        builder.addAllIn(exp.getInList());
        builder.setModifier(exp.getModifier());
        builder.setSensitivity(expSensitivity);
        switch (builder.getValueCase()) {
            case B:
                builder.setB(exp.getB());
                break;
            case I32:
                builder.setI32(exp.getI32());
                break;
            case I64:
                builder.setI64(exp.getI64());
                break;
            case F32:
                builder.setF32(exp.getF32());
                break;
            case F64:
                builder.setF64(exp.getF64());
                break;
            case STR:
                builder.setStr(exp.getStr());
                break;
            case BLOB:
                builder.setBlob(exp.getBlob());
                break;
        }
        return builder.build();
    }
}