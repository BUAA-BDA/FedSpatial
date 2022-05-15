package com.hufudb.onedb.expression;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionUtils {
  private final static Logger LOG = LoggerFactory.getLogger(ExpressionUtils.class);

  public static Schema createSchema(List<Expression> exps) {
    Schema.Builder builder = Schema.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", exp.getOutType(), exp.getModifier());
    });
    return builder.build();
  }

  public static List<Integer> getAggInputs(Expression agg) {
    assert agg.getOpType().equals(OperatorType.AGG_FUNC);
    return agg.getInList().stream().map(exp -> exp.getI32()).collect(Collectors.toList());
  }

  public static Object getLiteral(Expression lit) {
    switch (lit.getOutType()) {
      case BOOLEAN:
        return lit.getB();
      case STRING:
        return lit.getStr();
      case FLOAT:
        return lit.getF32();
      case DOUBLE:
        return lit.getF64();
      case BYTE:
      case SHORT:
      case INT:
        return lit.getI32();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return lit.getI64();
      default:
        throw new UnsupportedOperationException("Unsupported literal type");
    }
  }

  public static Expression conjunctCondition(List<Expression> filters) {
    final int size = filters.size();
    if (size == 1) {
      return filters.get(0);
    }
    Expression base = filters.get(0);
    for (int i = 1; i < size; ++i) {
      Expression filter = filters.get(i);
      base = Expression.newBuilder().setOpType(OperatorType.AND).setOutType(ColumnType.BOOLEAN)
          .setModifier(ModifierWrapper.dominate(base.getModifier(), filter.getModifier()))
          .addIn(base).addIn(filter).build();
    }
    return base;
  }
}
