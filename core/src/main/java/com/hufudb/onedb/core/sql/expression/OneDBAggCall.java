package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.TypeConverter;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.AggregateCall;

public class OneDBAggCall implements OneDBExpression {
  AggregateType aggType;
  List<Integer> in;
  ColumnType outType;
  Level level;
  boolean distinct;

  OneDBAggCall(AggregateType aggType, List<Integer> args, ColumnType type, Level level,
      boolean distinct) {
    this.aggType = aggType;
    this.in = args;
    this.outType = type;
    this.level = level;
    this.distinct = distinct;
  }

  OneDBAggCall(AggregateType aggType, List<Integer> args, ColumnType type, Level level) {
    this(aggType, args, type, level, false);
  }

  static Level getAggLevel(List<Integer> inputs, List<Level> levels) {
    Level res = Level.PUBLIC;
    for (int in : inputs) {
      res = Level.dominate(res, levels.get(in));
    }
    return res;
  }

  public static List<OneDBExpression> fromAggregates(List<AggregateCall> calls,
      List<Level> levels) {
    return calls.stream().map(call -> {
      AggregateType aggType = AggregateType.of(call);
      ColumnType outType = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBAggCall(aggType, new ArrayList<>(call.getArgList()), outType,
          getAggLevel(call.getArgList(), levels), call.isDistinct());
    }).collect(Collectors.toList());
  }

  public static List<OneDBExpression> fromGroups(List<Integer> groups, List<ColumnType> outTypes, List<Level> outLevels) {
    return groups.stream()
        .map(g -> new OneDBAggCall(AggregateType.GROUPKEY, Arrays.asList(g), outTypes.get(g), outLevels.get(g)))
        .collect(Collectors.toList());
  }

  public static OneDBAggCall fromProto(ExpressionProto proto) {
    if (!OneDBOpType.of(proto.getOpType()).equals(OneDBOpType.AGG_FUNC)) {
      throw new RuntimeException("not aggregate");
    }
    List<Integer> inputs =
        proto.getInList().stream().map(in -> in.getI32()).collect(Collectors.toList());
    return new OneDBAggCall(AggregateType.of(proto.getFunc()), inputs,
        ColumnType.of(proto.getOutType()), Level.of(proto.getLevel()), proto.getB());
  }

  @Override
  public ExpressionProto toProto() {
    return ExpressionProto.newBuilder().setOpType(OneDBOpType.AGG_FUNC.ordinal())
        .setFunc(aggType.ordinal()).setOutType(outType.ordinal()).setLevel(level.getId())
        .addAllIn(in.stream().map(i -> OneDBReference.fromIndex(outType, level, i).toProto())
            .collect(Collectors.toList()))
        .setB(distinct).build();
  }

  @Override
  public ColumnType getOutType() {
    return outType;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.AGG_FUNC;
  }

  @Override
  public Level getLevel() {
    return level;
  }

  public AggregateType getAggType() {
    return aggType;
  }

  public List<Integer> getInputRef() {
    return in;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public static OneDBAggCall create(AggregateType type, List<Integer> in, ColumnType out, Level level) {
    return new OneDBAggCall(type, in, out, level);
  }

  public enum AggregateType {
    GROUPKEY("GROUPKEY"), // used for group by key
    COUNT("COUNT"), AVG("AVG"), MAX("MAX"), MIN("MIN"), SUM("SUM"), UNSUPPORT("UNSUPPORT");

    private static final Map<String, AggregateType> MAP = new HashMap<>();

    static {
      for (AggregateType type : values()) {
        MAP.put(type.name, type);
      }
    }

    private final String name;

    AggregateType(String name) {
      this.name = name;
    }

    public static AggregateType of(String typeString) {
      return MAP.get(typeString);
    }

    public static AggregateType of(int id) {
      return values()[id];
    }

    public static AggregateType of(AggregateCall call) {
      switch (call.getAggregation().getKind()) {
        case COUNT:
          return MAP.get("COUNT");
        case AVG:
          return MAP.get("AVG");
        case MAX:
          return MAP.get("MAX");
        case MIN:
          return MAP.get("MIN");
        case SUM:
          return MAP.get("SUM");
        default:
          return MAP.get("UNSUPPORT");
      }
    }
  }
}
