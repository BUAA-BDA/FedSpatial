package com.hufudb.openhufu.owner;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.checker.Checker;
import com.hufudb.openhufu.owner.config.ImplementorConfig;
import com.hufudb.openhufu.owner.config.OwnerConfig;
import com.hufudb.openhufu.owner.implementor.OwnerSideImplementor;
import com.hufudb.openhufu.owner.storage.StreamDataSet;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.data.schema.PublishedTableSchema;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.proto.ServiceGrpc;
import com.hufudb.openhufu.proto.OpenHuFuData.DataSetProto;
import com.hufudb.openhufu.proto.OpenHuFuData.SchemaProto;
import com.hufudb.openhufu.proto.OpenHuFuData.TableSchemaListProto;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import com.hufudb.openhufu.proto.OpenHuFuService.GeneralRequest;
import com.hufudb.openhufu.proto.OpenHuFuService.GeneralResponse;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class OwnerService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerService.class);
  protected final String endpoint;
  protected final ExecutorService threadPool;
  protected final OpenHuFuRpc ownerSideRpc;
  protected final OwnerSideImplementor implementor;
  protected final Adapter adapter;
  protected final Jedis jedis;
  protected final Map<ProtocolType, ProtocolExecutor> libraries;
  protected final SchemaManager schemaManager;

  public OwnerService(OwnerConfig config) {
    this.threadPool = config.threadPool;
    this.endpoint = String.format("%s:%d", config.hostname, config.port);
    this.ownerSideRpc = config.acrossOwnerRpc;
    this.adapter = config.adapter;
    this.implementor = new OwnerSideImplementor(ownerSideRpc, adapter, threadPool);
    this.schemaManager = this.adapter.getSchemaManager();
    this.libraries = config.librarys;
    this.jedis = config.jedis;
    System.out.println(jedis.ping());
    System.out.println(jedis.get("blockHeight"));
    ImplementorConfig.initImplementorConfig(config.implementorConfigPath);
    initPublishedTable(config.tables);
  }

  @Override
  public void query(QueryPlanProto request, StreamObserver<DataSetProto> responseObserver) {
    Plan plan = Plan.fromProto(request);
    LOG.info("receives plan:\n{}", plan);
    if (!Checker.check(plan, schemaManager)) {
      LOG.warn("Check fail for plan {}", request);
      responseObserver.onCompleted();
      return;
    }
    try {
      DataSet result = implementor.implement(plan);
      StreamDataSet output = new StreamDataSet(result, responseObserver);
      output.stream();
      output.close();
    } catch (Exception e) {
      throw new OpenHuFuException(e, ErrorCode.QUERY_ERROR);
    }
  }

  @Override
  public void getOwnerInfo(GeneralRequest request,
      StreamObserver<OwnerInfo> responseObserver) {
    Party party = ownerSideRpc.ownParty();
    LOG.info("Get owner info {}", party);
    responseObserver.onNext(
        OwnerInfo.newBuilder().setId(party.getPartyId()).setEndpoint(endpoint).build());
    responseObserver.onCompleted();
  }

  @Override
  public void addOwner(OwnerInfo request, StreamObserver<GeneralResponse> responseObserver) {
    LOG.info("Connect to owner {}", OpenHuFuOwnerInfo.fromProto(request));
    boolean ok = ownerSideRpc.addParty(OpenHuFuOwnerInfo.fromProto(request));
    ownerSideRpc.connect();
    responseObserver.onNext(GeneralResponse.newBuilder().setStatus(ok ? 0 : 1)
        .setMsg(ok ? "" : "Fail to add owner").build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTableSchema(GeneralRequest request, StreamObserver<SchemaProto> responseObserver) {
    Schema fakeSchema = getPublishedTableHeader(request.getValue());
    SchemaProto schemaProto = fakeSchema.toProto();
    LOG.info("Get schema of table {} {}", request.getValue(), fakeSchema);
    responseObserver.onNext(schemaProto);
    responseObserver.onCompleted();
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public OpenHuFuRpc getOwnerSideRpc() {
    return ownerSideRpc;
  }

  public List<PublishedTableSchema> getAllPublishedTable() {
    return schemaManager.getAllPublishedTable();
  }

  @Override
  public void getAllTableSchema(GeneralRequest request,
      StreamObserver<TableSchemaListProto> responseObserver) {
    TableSchemaListProto.Builder builder = TableSchemaListProto.newBuilder();
    getAllPublishedTable().forEach(info -> builder.addTable(info.getFakeTableSchema().toProto()));
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table schemas", builder.getTableCount());
    responseObserver.onCompleted();
  }

  protected String getLocalTableName(String publishedTableName) {
    return schemaManager.getActualTableName(publishedTableName);
  }

  protected Schema getPublishedTableHeader(String publishedTableName) {
    return schemaManager.getPublishedSchema(publishedTableName);
  }

  public TableSchema getLocalTableSchema(String tableName) {
    return schemaManager.getLocalTable(tableName);
  }

  public List<TableSchema> getAllLocalTable() {
    return schemaManager.getAllLocalTable();
  }

  public void clearPublishedTable() {
    schemaManager.clearPublishedTable();
  }

  public void dropPublishedTable(String tableName) {
    schemaManager.dropPublishedTable(tableName);
  }

  public void initPublishedTable(List<PojoPublishedTableSchema> schemas) {
    if (schemas == null) {
      return;
    }
    for (PojoPublishedTableSchema schema : schemas) {
      schemaManager.addPublishedTable(schema);
    }
  }

  public boolean addPublishedTable(PojoPublishedTableSchema schema) {
    return schemaManager.addPublishedTable(schema);
  }

  public boolean changeCatalog(String catalog) {
    LOG.error("change catalog operation is not supported in your database");
    return false;
  }

  protected void shutdown() {
    adapter.shutdown();
  }
}
