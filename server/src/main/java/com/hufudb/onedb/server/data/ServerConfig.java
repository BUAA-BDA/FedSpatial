package com.hufudb.onedb.server.data;

import com.hufudb.onedb.core.data.Level;
import java.util.List;

public interface ServerConfig {

  public abstract Table getTable(String tableName);

  public static class Table {
    public String name;
    public List<Column> columns;
    public List<Mapping> mappings;

    public Level getLevel(String columnName) {
      for (Column column : columns) {
        if (columnName.equals(column.name)) {
          return column.level;
        }
      }
      return Level.PUBLIC;
    }
  }

  public static class Column {
    public String name;
    public Level level;
  }

  public static class Mapping {
    public String schema;
    public String name;
  }
}
