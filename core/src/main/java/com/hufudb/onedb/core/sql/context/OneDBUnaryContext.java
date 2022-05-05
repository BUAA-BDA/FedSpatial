package com.hufudb.onedb.core.sql.context;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;
import org.apache.commons.lang3.tuple.Pair;

/*
 * context for intermediate process with single input relation (e.g., outer layer of nested
 * aggregation)
 */
public class OneDBUnaryContext extends OneDBBaseContext {
  OneDBContext parent;
  OneDBContext child;
  List<OneDBExpression> aggExps;
  List<OneDBExpression> selectExps;
  List<Integer> groups;
  List<OneDBOrder> orders;
  int fetch;
  int offset;

  public OneDBUnaryContext() {
    super();
  }

  @Override
  public List<Pair<OwnerClient, QueryContextProto>> generateOwnerContextProto(OneDBClient client) {
    QueryContextProto.Builder contextBuilder = QueryContextProto.newBuilder().setContextType(OneDBContextType.UNARY.ordinal()).setFetch(fetch).setOffset(offset);
    if (selectExps != null) {
      contextBuilder.addAllSelectExp(OneDBExpression.toProto(selectExps));
    }
    if (aggExps != null) {
      contextBuilder.addAllAggExp(OneDBExpression.toProto(aggExps));
    }
    if (groups != null) {
      contextBuilder.addAllGroup(groups);
    }
    if (orders != null) {
      contextBuilder.addAllOrder(OneDBOrder.toProto(orders));
    }
    List<Pair<OwnerClient, QueryContextProto>> ownerContext = child.generateOwnerContextProto(client);
    // todo: generate task info for each expression
    TaskInfoProto.Builder taskInfo = TaskInfoProto.newBuilder().setTaskId(client.getTaskId());
    for (Pair<OwnerClient, QueryContextProto> p : ownerContext) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    contextBuilder.setTaskInfo(taskInfo);
    for (Pair<OwnerClient, QueryContextProto> p : ownerContext) {
      QueryContextProto context = contextBuilder.addChildren(p.getValue()).build();
      p.setValue(context);
    }
    return ownerContext;
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
    } else if (selectExps != null && !selectExps.isEmpty()) {
      return selectExps;
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
  public void setSelectExps(List<OneDBExpression> selectExps) {
    this.selectExps = selectExps;
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
  public List<OneDBOrder> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<OneDBOrder> orders) {
    this.orders = orders;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream()
        .map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Level getContextLevel() {
    return Level.findDominator(getOutExpressions());
  }

  @Override
  public List<Level> getOutLevels() {
    return getOutExpressions().stream().map(exp -> exp.getLevel()).collect(Collectors.toList());
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
    if (selectExps != null && !selectExps.isEmpty()) {
      input = input.project(implementor, selectExps);
    }
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

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    this.child = child.rewrite(rewriter);
    return rewriter.rewriteUnary(this);
  }
}
