package com.hufudb.onedb.data.storage;

import java.util.List;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.function.Mapping;

/**
 * Dataset which map each row from source dataset into a new row
 */
public class MapDataSet implements DataSet {
  private final Schema schema;
  private final List<Mapping> mappings;
  private final DataSet source;

  MapDataSet(Schema schema, List<Mapping> mappings, DataSet source) {
    this.schema = schema;
    this.mappings = mappings;
    this.source = source;
  }

  public static MapDataSet create(Schema schema, List<Mapping> mappings, DataSet source) {
    return new MapDataSet(schema, mappings, source);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new MapIterator(source.getIterator());
  }

  @Override
  public void close() {
    source.close();
  }

  class MapIterator implements DataSetIterator {
    DataSetIterator iterator;

    MapIterator(DataSetIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Object get(int columnIndex) {
      return mappings.get(columnIndex).map(iterator);
    }
  }
}
