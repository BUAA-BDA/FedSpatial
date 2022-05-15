package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public class PlaintextCalculator {
  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> calcs) {
    List<ColumnType> types = input.getTypeList();
    input.getRows().replaceAll(row -> calcRow(row, types, calcs));
    return input;
  }

  public static Row calcRow(Row row, List<ColumnType> types, List<OneDBExpression> calcs) {
    final int length = calcs.size();
    RowBuilder builder = Row.newBuilder(length);
    for (int i = 0; i < length; ++i) {
      builder.set(
          i,
          PlaintextInterpreter.cast(
              PlaintextInterpreter.implement(row, calcs.get(i)), calcs.get(i).getOutType()));
    }
    return builder.build();
  }
}
