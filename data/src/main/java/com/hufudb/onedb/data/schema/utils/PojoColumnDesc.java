package com.hufudb.onedb.data.schema.utils;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;

public class PojoColumnDesc {
  public String name;
  public ColumnTypeWrapper type;
  public ModifierWrapper modifier;

  public ColumnType getType() {
    return type.get();
  }

  public Modifier getModifier() {
    return modifier.get();
  }

  public static PojoColumnDesc fromColumnDesc(ColumnDesc col) {
    PojoColumnDesc pcol = new PojoColumnDesc();
    pcol.name = col.getName();
    pcol.type = ColumnTypeWrapper.of(col.getType());
    pcol.modifier = ModifierWrapper.of(col.getModifier());
    return pcol;
  }

  public static List<PojoColumnDesc> fromColumnDesc(List<ColumnDesc> colums) {
    return colums.stream().map(col -> fromColumnDesc(col)).collect(Collectors.toList());
  }

  public ColumnDesc toColumnDesc() {
    return ColumnDesc.newBuilder().setName(name).setType(getType()).setModifier(getModifier())
        .build();
  }
}
