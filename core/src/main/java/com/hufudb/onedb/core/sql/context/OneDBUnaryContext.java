package com.hufudb.onedb.core.sql.context;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

/*
 * context for intermediate process with single input relation (e.g., outer layer of nested
 * aggregation)
 */
public class OneDBUnaryContext extends OneDBBaseContext {
  OneDBContext parent;
  OneDBContext child;
  List<OneDBExpression> aggExps;
  List<Integer> groups;
  List<String> orders;
  int fetch;
  int offset;

  public OneDBUnaryContext() {
    super();
  }

  @Override
  public OneDBContext getParent() {
    return parent;
  }

  @Override
  public void setParent(OneDBContext parent) {
    this.parent = parent;
  }

  @Override
  public List<OneDBContext> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void setChildren(List<OneDBContext> children) {
    assert children.size() == 1;
    this.child = children.get(0);
  }

  @Override
  public void updateChild(OneDBContext newChild, OneDBContext oldChild) {
    assert oldChild == child;
    child = newChild;
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    if (aggExps != null && !aggExps.isEmpty()) {
      return aggExps;
    } else {
      LOG.error("Unary context without output expression");
      throw new RuntimeException("Unary context without output expression");
    }
  }

  @Override
  public List<OneDBExpression> getAggExps() {
    return aggExps;
  }

  @Override
  public void setAggExps(List<OneDBExpression> aggExps) {
    this.aggExps = aggExps;
  }

  @Override
  public List<Integer> getGroups() {
    return groups;
  }

  @Override
  public void setGroups(List<Integer> groups) {
    this.groups = groups;
  }

  @Override
  public List<String> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<String> orders) {
    this.orders = orders;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream()
        .map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Level getOutLevel() {
    return Level.findDominator(getOutExpressions());
  }

  @Override
  public int getFetch() {
    return fetch;
  }

  @Override
  public void setFetch(int fetch) {
    this.fetch = fetch;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.UNARY;
  }

  public QueryableDataSet implementInternal(OneDBImplementor implementor, QueryableDataSet input) {
    if (aggExps != null && !aggExps.isEmpty()) {
      input = input.aggregate(implementor, groups, aggExps, child.getOutTypes());
    }
    if (orders != null && !orders.isEmpty()) {
      input = input.sort(implementor, orders);
    }
    if (fetch > 0 || offset > 0) {
      input = input.limit(offset, fetch);
    }
    return input;
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    QueryableDataSet input = child.implement(implementor);
    return implementInternal(implementor, input);
  }
}
