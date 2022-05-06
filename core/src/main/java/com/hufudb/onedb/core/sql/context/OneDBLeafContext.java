package com.hufudb.onedb.core.sql.context;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

/*
 * context for single global table query (horizontal partitioned table)
 */
public class OneDBLeafContext extends OneDBBaseContext {
  OneDBContext parent;
  String tableName;
  List<OneDBExpression> selectExps = new ArrayList<>();
  List<OneDBExpression> whereExps = new ArrayList<>();
  List<OneDBExpression> aggExps = new ArrayList<>();
  List<Integer> groups = new ArrayList<>();
  List<OneDBOrder> orders = new ArrayList<>();
  int fetch;
  int offset;

  public OneDBLeafContext() {
    super();
  }

  @Override
  public List<Pair<OwnerClient, QueryContextProto>> generateOwnerContextProto(OneDBClient client) {
    // there is no task info for leaf query
    QueryContextProto.Builder contextBuilder =
        QueryContextProto.newBuilder().setContextType(OneDBContextType.LEAF.ordinal())
            .addAllSelectExp(OneDBExpression.toProto(selectExps)).setFetch(fetch).setOffset(offset);
    if (whereExps != null) {
      contextBuilder.addAllWhereExp(OneDBExpression.toProto(whereExps));
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
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(tableName);
    List<Pair<OwnerClient, QueryContextProto>> ownerContext = new ArrayList<>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      contextBuilder.setTableName(entry.getRight());
      ownerContext.add(MutablePair.of(entry.getLeft(), contextBuilder.build()));
    }
    return ownerContext;
  }

  public static OneDBLeafContext fromProto(QueryContextProto proto) {
    OneDBLeafContext context = new OneDBLeafContext();
    context.setTableName(proto.getTableName());
    context.setSelectExps(OneDBExpression.fromProto(proto.getSelectExpList()));
    context.setWhereExps(OneDBExpression.fromProto(proto.getWhereExpList()));
    context.setAggExps(OneDBExpression.fromProto(proto.getAggExpList()));
    context.setGroups(proto.getGroupList());
    context.setOrders(OneDBOrder.fromProto(proto.getOrderList()));
    context.setFetch(proto.getFetch());
    context.setOffset(proto.getOffset());
    return context;
  }

  public QueryContextProto toProto() {
    QueryContextProto.Builder builder = QueryContextProto.newBuilder();
    builder.setContextType(OneDBContextType.LEAF.ordinal()).setTableName(tableName)
        .addAllSelectExp(OneDBExpression.toProto(selectExps)).setFetch(fetch).setOffset(offset);
    if (whereExps != null) {
      builder.addAllWhereExp(OneDBExpression.toProto(whereExps));
    }
    if (aggExps != null) {
      builder.addAllAggExp(OneDBExpression.toProto(aggExps));
    }
    if (groups != null) {
      builder.addAllGroup(groups);
    }
    if (orders != null) {
      builder.addAllOrder(OneDBOrder.toProto(orders));
    }
    return builder.build();
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.LEAF;
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
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String name) {
    this.tableName = name;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
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
  public List<OneDBExpression> getOutExpressions() {
    if (aggExps != null && !aggExps.isEmpty()) {
      return aggExps;
    } else if (selectExps != null && !selectExps.isEmpty()) {
      return selectExps;
    } else {
      LOG.error("Leaf context without output expression");
      throw new RuntimeException("Leaf context without output expression");
    }
  }

  @Override
  public List<OneDBExpression> getSelectExps() {
    return selectExps;
  }

  @Override
  public void setSelectExps(List<OneDBExpression> selectExps) {
    this.selectExps = selectExps;
  }

  @Override
  public List<OneDBExpression> getWhereExps() {
    return whereExps;
  }

  @Override
  public void setWhereExps(List<OneDBExpression> whereExps) {
    this.whereExps = whereExps;
  }

  @Override
  public boolean hasAgg() {
    return aggExps != null && !aggExps.isEmpty();
  }

  @Override
  public List<OneDBExpression> getAggExps() {
    return aggExps;
  }

  @Override
  public void setAggExps(List<OneDBExpression> aggExps) {
    if (this.aggExps != null && !this.aggExps.isEmpty()) {
      LOG.error("leaf query does not support aggregate function nesting");
      throw new RuntimeException("leaf query does not support aggregate function nesting");
    }
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
  public List<OneDBOrder> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<OneDBOrder> orders) {
    this.orders = orders;
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

  public List<FieldType> getSelectTypes() {
    return selectExps.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    return implementor.leafQuery(this);
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    return rewriter.rewriteLeaf(this);
  }
}
