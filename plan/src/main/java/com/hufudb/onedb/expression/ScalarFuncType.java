package com.hufudb.onedb.expression;

import com.google.common.collect.ImmutableMap;

public enum ScalarFuncType {
  ABS("ABS", 1, Number.class);

  private final static ImmutableMap<Integer, ScalarFuncType> MAP;

  private final String name;
  private final int id;
  private final Object[] argumentTypes;

  static {
    final ImmutableMap.Builder<Integer, ScalarFuncType> builder = ImmutableMap.builder();
    for (ScalarFuncType func : values()) {
      builder.put(func.getId(), func);
    }
    MAP = builder.build();
  }

  ScalarFuncType(String name, int id, Class... argumentTypes) {
    this.name = name;
    this.id = id;
    this.argumentTypes = argumentTypes;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }
  
  public static ScalarFuncType of(int id) {
    return MAP.get(id);
  }
}
