package com.hufudb.onedb.core.table.utils;

import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.data.schema.utils.PojoSchema;
import java.util.ArrayList;
import java.util.List;

public class PojoGlobalTableSchema {
  String name;
  PojoSchema schema;
  List<LocalTableConfig> mappings;

  public static PojoGlobalTableSchema from(OneDBTableSchema info) {
    PojoGlobalTableSchema sinfo = new PojoGlobalTableSchema();
    sinfo.setName(info.getName());
    sinfo.setSchema(PojoSchema.fromSchema(info.getSchema()));
    sinfo.setMappings(info.getMappings());
    return sinfo;
  }

  public static List<PojoGlobalTableSchema> from(List<OneDBTableSchema> info) {
    List<PojoGlobalTableSchema> sinfo = new ArrayList<>();
    for (OneDBTableSchema i : info) {
      sinfo.add(PojoGlobalTableSchema.from(i));
    }
    return sinfo;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public PojoSchema getSchema() {
    return schema;
  }

  public void setSchema(PojoSchema schema) {
    this.schema = schema;
  }

  public List<LocalTableConfig> getMappings() {
    return mappings;
  }

  public void setMappings(List<LocalTableConfig> mappings) {
    this.mappings = mappings;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
