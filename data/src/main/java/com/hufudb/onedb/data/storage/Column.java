package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.proto.OneDBData.ColumnType;

public interface Column {
  Object getObject(int rowNum);
  ColumnType getType();
  int size();

  interface CellGetter {
    Object get(int rowNum);
  }
}
