package com.hufudb.onedb.core.sql.implementor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaintextImplementor {
  private static final Logger LOG = LoggerFactory.getLogger(PlaintextImplementor.class);

  private final OneDBClient client;
  private final ExecutorService executorService;

  public PlaintextImplementor(OneDBClient client) {
    this.client = client;
    this.executorService = Executors.newFixedThreadPool(OneDBConfig.CLIENT_THREAD_NUM);
  }

  public QueryableDataSet implement(OneDBQueryProto proto) {
    if (isLeaf(proto)) {
      return implementSingle(proto);
    } else {
      QueryableDataSet left = implement(proto.getLeft());
      QueryableDataSet right = implement(proto.getRight());
      return implementBinary(proto, left, right);
    }
  }

  // for single query, where and select has already implemented in owner side
  // only need to implement aggregate, sort and limit
  QueryableDataSet implementSingle(OneDBQueryProto proto) {
    String tableName = proto.getTableName();
    assert !tableName.isEmpty();
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(tableName);
    StreamBuffer<DataSetProto> streamProto = tableQuery(proto, tableClients);
    Header header = OneDBExpression.generateHeader(proto.getSelectExpList());
    // todo: optimze for streamDataSet
    BasicDataSet localDataSet = BasicDataSet.of(header);
    while (streamProto.hasNext()) {
      localDataSet.mergeDataSet(BasicDataSet.fromProto(streamProto.next()));
    }
    QueryableDataSet queryable = QueryableDataSet.fromBasic(localDataSet);
    if (proto.getAggExpCount() > 0) {
      queryable.aggregate(proto.getAggExpList());
    }
    // todo: add sort and limit
    return queryable;
  }

  QueryableDataSet implementBinary(OneDBQueryProto proto, QueryableDataSet left, QueryableDataSet right) {
    QueryableDataSet dataSet = QueryableDataSet.join(left, right, new OneDBJoinInfo(proto));
    if (proto.getWhereExpCount() > 0) {
      dataSet.filter(proto.getWhereExpList());
    }
    if (proto.getSelectExpCount() > 0) {
      dataSet.filter(proto.getSelectExpList());
    }
    if (proto.getAggExpCount() > 0) {
      dataSet.filter(proto.getAggExpList());
    }
    // todo: add sort and limit
    return dataSet;
  }

  private StreamBuffer<DataSetProto> tableQuery(OneDBQueryProto query, List<Pair<OwnerClient, String>> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        try {
          OneDBQueryProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().oneDBQuery(localQuery);
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = executorService.invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in oneDBQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error in OneDBQuery for {}", e.getMessage());
    }
    return iterator;
  }

  boolean isLeaf(OneDBQueryProto proto) {
    return !(proto.hasLeft() && proto.hasRight());
  }
}
