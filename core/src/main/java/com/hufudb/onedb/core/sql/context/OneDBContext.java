package com.hufudb.onedb.core.sql.context;

import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OneDBContext {
  static final Logger LOG = LoggerFactory.getLogger(OneDBContext.class);

  OneDBContextType getContextType();

  List<OneDBExpression> getOutExpressions();

  List<FieldType> getOutTypes();

  Level getContextLevel();

  List<Level> getOutLevels();

  OneDBContext getParent();

  void setParent(OneDBContext parent);

  List<OneDBContext> getChildren();

  void setChildren(List<OneDBContext> children);

  void updateChild(OneDBContext newChild, OneDBContext oldChild);

  String getTableName();

  void setTableInfo(OneDBTableInfo info);

  List<OneDBExpression> getSelectExps();

  void setSelectExps(List<OneDBExpression> selectExps);

  List<OneDBExpression> getWhereExps();

  void setWhereExps(List<OneDBExpression> whereExps);

  List<OneDBExpression> getAggExps();

  void setAggExps(List<OneDBExpression> aggExps);

  boolean hasAgg();

  List<Integer> getGroups();

  void setGroups(List<Integer> groups);

  List<OneDBOrder> getOrders();

  void setOrders(List<OneDBOrder> orders);

  int getFetch();

  void setFetch(int fetch);

  int getOffset();

  void setOffset(int offset);

  OneDBJoinInfo getJoinInfo();

  void setJoinInfo(OneDBJoinInfo joinInfo);

  QueryableDataSet implement(OneDBImplementor implementor);

  OneDBContext rewrite(OneDBRewriter rewriter);

  Set<OwnerClient> getOwners();

  public static Header getOutputHeader(QueryContextProto proto) {
    Header.Builder builder = Header.newBuilder();
    List<FieldType> types = getOutputTypes(proto);
    types.stream().forEach(type -> builder.add("", type));
    return builder.build();
  }

  public static List<FieldType> getOutputTypes(QueryContextProto proto) {
    if (proto.getAggExpCount() > 0) {
      return proto.getAggExpList().stream().map(agg -> FieldType.of(agg.getOutType()))
          .collect(Collectors.toList());
    } else {
      return proto.getSelectExpList().stream().map(sel -> FieldType.of(sel.getOutType()))
          .collect(Collectors.toList());
    }
  }

  public static List<FieldType> getOutputTypes(QueryContextProto proto, List<Integer> indexs) {
    if (proto.getAggExpCount() > 0) {
      return indexs.stream().map(id -> FieldType.of(proto.getAggExp(id).getOutType()))
          .collect(Collectors.toList());
    } else {
      return indexs.stream().map(id -> FieldType.of(proto.getSelectExp(id).getOutType()))
          .collect(Collectors.toList());
    }
  }

  public static List<OneDBExpression> getOutputExpressions(QueryContextProto proto) {
    if (proto.getAggExpCount() > 0) {
      return OneDBExpression.fromProto(proto.getAggExpList());
    } else {
      return OneDBExpression.fromProto(proto.getSelectExpList());
    }
  }

  public static OneDBContext fromProto() {
    return null;
  }
}
