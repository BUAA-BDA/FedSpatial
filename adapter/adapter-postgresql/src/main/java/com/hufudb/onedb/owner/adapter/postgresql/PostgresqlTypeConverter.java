package com.hufudb.onedb.owner.adapter.postgresql;

import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class PostgresqlTypeConverter extends AdapterTypeConverter {
  @Override
  public ColumnType convert(int type, String typeName) {
    switch (typeName) {
      case "real":
      case "float4":
      return ColumnType.FLOAT;
      case "float8":
      case "double precision":
      case "numeric":
      return ColumnType.DOUBLE;
      case "TINYINT":
      return ColumnType.BYTE;
      case "SMALLINT":
      return ColumnType.SHORT;
      case "int2":
      case "int4":
      return ColumnType.INT;
      case "oid":
      case "int8":
      return ColumnType.LONG;
      case "varchar":
      case "char":
      case "bpchar":
      case "text":
      case "name":
      return ColumnType.STRING;
      case "bit":
      case "bool":
      return ColumnType.BOOLEAN;
      case "date":
      return ColumnType.DATE;
      case "time":
      return ColumnType.TIME;
      case "timestamp":
      return ColumnType.TIMESTAMP;
      default:
      return ColumnType.STRING;
    }
  }
}
