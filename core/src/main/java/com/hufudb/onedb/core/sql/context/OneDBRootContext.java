package com.hufudb.onedb.core.sql.context;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class OneDBRootContext extends OneDBBaseContext {
  private static final AtomicLong counter = new AtomicLong(0);

  final long id;
  OneDBContext child;

  public OneDBRootContext() {
    counter.compareAndSet(Long.MAX_VALUE, 0);
    id = counter.addAndGet(1);
  }

  public long getContextId() {
    return id;
  }

  public OneDBContext getChild() {
    return child;
  }

  public void setChild(OneDBContext child) {
    this.child = child;
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.ROOT;
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    return ImmutableList.of();
  }

  @Override
  public List<FieldType> getOutTypes() {
    return ImmutableList.of();
  }

  @Override
  public List<OneDBContext> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void setChildren(List<OneDBContext> children) {
    assert children.size() == 1;
    child = children.get(0);
  }

  @Override
  public void updateChild(OneDBContext newChild, OneDBContext oldChild) {
    assert oldChild == child;
    child = newChild;
  }
};
