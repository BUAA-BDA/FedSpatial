package com.hufudb.onedb.core.implementor.plaintext;


import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Schema;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import java.util.List;

public class PlaintextQueryableDataSet extends BasicDataSet implements QueryableDataSet {
  public static final PlaintextQueryableDataSet EMPTY = new PlaintextQueryableDataSet(Schema.EMPTY);

  protected PlaintextQueryableDataSet(Schema header) {
    super(header);
  }

  PlaintextQueryableDataSet(BasicDataSet dataSet) {
    super(dataSet.getHeader(), dataSet.getRows());
  }

  public static QueryableDataSet fromBasic(BasicDataSet dataSet) {
    return new PlaintextQueryableDataSet(dataSet);
  }

  public static QueryableDataSet fromHeader(Schema header) {
    return new PlaintextQueryableDataSet(header);
  }

  public static QueryableDataSet fromExpression(List<OneDBExpression> exps) {
    Schema header = OneDBExpression.generateHeader(exps);
    return new PlaintextQueryableDataSet(header);
  }

  @Override
  public List<ColumnType> getTypeList() {
    return header.getTypeList();
  }

  @Override
  public QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return implementor.join(this, right, joinInfo);
  }

  @Override
  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    return implementor.filter(this, filters);
  }

  @Override
  public QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects) {
    return implementor.project(this, projects);
  }

  @Override
  public QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<ColumnType> inputTypes) {
    return implementor.aggregate(this, groups, aggs, inputTypes);
  }

  @Override
  public QueryableDataSet sort(OneDBImplementor implementor, List<OneDBOrder> orders) {
    return implementor.sort(this, orders);
  }

  @Override
  public QueryableDataSet limit(int offset, int fetch) {
    if (fetch == 0) {
      if (offset >= this.getRowCount()) {
        this.rows.clear();
      } else {
        this.rows = this.rows.subList(offset, this.getRowCount());
      }
    } else {
      if (offset >= this.getRowCount()) {
        this.rows.clear();
      } else {
        this.rows = this.rows.subList(offset, Math.min(this.getRowCount(), offset + fetch));
      }
    }
    return this;
  }
}
