package com.hufudb.openhufu.owner;

import com.hufudb.openhufu.core.config.wyx_task.WXY_ConfigFile;
import com.hufudb.openhufu.core.config.wyx_task.WXY_Output;
import com.hufudb.openhufu.core.config.wyx_task.WXY_OutputDataItem;
import com.hufudb.openhufu.data.schema.utils.PojoColumnDesc;
import com.hufudb.openhufu.data.storage.*;
import com.hufudb.openhufu.owner.adapter.Adapter;
import com.hufudb.openhufu.owner.checker.Checker;
import com.hufudb.openhufu.owner.config.ImplementorConfig;
import com.hufudb.openhufu.owner.config.OwnerConfig;
import com.hufudb.openhufu.owner.config.PostgisConfig;
import com.hufudb.openhufu.owner.implementor.OwnerSideImplementor;
import com.hufudb.openhufu.owner.storage.StreamDataSet;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.data.schema.PublishedTableSchema;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnProto;
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

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerService.class);
  protected final String endpoint;
  protected final ExecutorService threadPool;
  protected final OpenHuFuRpc ownerSideRpc;
  protected final OwnerSideImplementor implementor;
  protected final Adapter adapter;
  protected final Map<ProtocolType, ProtocolExecutor> libraries;
  protected final SchemaManager schemaManager;
  protected final PostgisConfig postgisConfig;

  protected final Schema schema;
  protected final WXY_ConfigFile wxy_configFile;

  protected WXY_OutputDataItem outputDataItem = null;


  public OwnerService(OwnerConfig config) {
    this.wxy_configFile = config.wxy_configFile;
    this.threadPool = config.threadPool;
    String hostname = System.getenv("hostname");
    String port = System.getenv("nodePort");
    this.endpoint = hostname + ":" + port;
    // this.endpoint = String.format("%s:%d", config.hostname, config.port);
    this.ownerSideRpc = config.acrossOwnerRpc;
    this.adapter = config.adapter;
    this.implementor = new OwnerSideImplementor(ownerSideRpc, adapter, threadPool);
    this.schemaManager = this.adapter.getSchemaManager();
    this.libraries = config.librarys;
    this.postgisConfig = config.postgisConfig;
    Schema.Builder schemaBuilder = Schema.newBuilder();
    ImplementorConfig.initImplementorConfig(config.implementorConfigPath);
    initPublishedTable(config.tables);
    for (PojoColumnDesc pojoColumnDesc : config.tables.get(0).publishedColumns) {
      schemaBuilder.add(pojoColumnDesc.toColumnDesc());
    }
    this.schema = schemaBuilder.build();
    for (WXY_OutputDataItem outputDataItem : wxy_configFile.output.getData()) {
      if (outputDataItem.getDomainID().equals(System.getenv("orgDID"))) {
          this.outputDataItem = outputDataItem;
          break;
      }
    }
  }

  @Override
  public void query(QueryPlanProto request, StreamObserver<DataSetProto> responseObserver) {
    Plan plan = Plan.fromProto(request);
    LOG.info("receives plan:\n{}", plan);
    if (!Checker.check(plan, schemaManager)) {
      LOG.warn("Check fail for plan {}", request.toString());
      responseObserver.onCompleted();
      return;
    }
    try {
      DataSet result = null;
      ArrayDataSet tempDataset = null;
      StreamDataSet output = null;
      switch (wxy_configFile.module.getModuleName()) {
        case "KNN":
          result = implementor.implement(plan);
          output = new StreamDataSet(result, responseObserver);
          break;
        case "RANGEQUERY":
          result = implementor.leafQuery((LeafPlan) plan);
          tempDataset = ArrayDataSet.materialize(schema, result);
          saveResult(tempDataset);
          output = new StreamDataSet(EmptyDataSet.INSTANCE, responseObserver);
          break;
        case "RANGECOUNT":
          result = implementor.leafQuery((LeafPlan) ((UnaryPlan) plan).getChildren().get(0));
          tempDataset =  ArrayDataSet.materialize(Schema.newBuilder()
                  .add(OpenHuFuData.ColumnDesc.newBuilder()
                          .setName("local_count")
                          .setModifier(OpenHuFuData.Modifier.PUBLIC)
                          .setType(OpenHuFuData.ColumnType.LONG).build()).build(),
                  result);
          saveResult(tempDataset);
          output = new StreamDataSet(EmptyDataSet.INSTANCE, responseObserver);
          break;
        default:
          LOG.error("not support module {}", wxy_configFile.module.getModuleName());
      }
      output.stream();
      output.close();
    } catch (Exception e) {
      LOG.error("Error in query", e);
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

  @Override
  public void saveResult(DataSetProto dataSetProto, StreamObserver<GeneralResponse> responseObserver) {
    try {
      saveResult(ProtoDataSet.create(dataSetProto));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    responseObserver.onNext(GeneralResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  private void saveResult(DataSet result) throws SQLException {
    LOG.info("save result");
    if (outputDataItem == null) {
      return;
    }
    if (outputDataItem.getFinalResult().equals("Y")) {
      saveResult2Minio(result);
    } else if (outputDataItem.getFinalResult().equals("N")) {
      saveResult2PG(outputDataItem.getDataName(), result);
    }
  }

  private void saveResult2PG(String tableName, DataSet result) throws SQLException {
    Connection connection = DriverManager
            .getConnection(postgisConfig.jdbcUrl, postgisConfig.user, postgisConfig.password);
    Statement statement = connection.createStatement();
    //todo now all the columns are stored as string
    //step1 create table
    String checkTableQuery = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + tableName + "')";
    ResultSet resultSet = statement.executeQuery(checkTableQuery);

    if (resultSet.next() && resultSet.getBoolean(1)) {
      String dropTableQuery = "DROP TABLE " + tableName;
      statement.executeUpdate(dropTableQuery);
      LOG.info("table {} exists, deleting", tableName);
    }
    Schema schema = result.getSchema();
    int columnCount = schema.getColumnDescs().size();
    String createTableSql = "CREATE TABLE "
            + tableName + " (";
    for (int i = 0; i < columnCount - 1; i++) {
      createTableSql = createTableSql
              + schema.getColumnDesc(i).getName()
              + " varchar(255), ";
    }
    createTableSql = createTableSql
            + schema.getColumnDesc(columnCount - 1).getName()
            + " varchar(255))";
    LOG.info("executing SQL: {}", createTableSql);
    statement.executeUpdate(createTableSql);

    //step2 insert records
    String insertSql = "insert into " + tableName + " values (";
    for (int i = 1; i < columnCount; i++) {
      insertSql = insertSql + "?, ";
    }
    insertSql = insertSql + "?)";
    PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
    DataSetIterator it = result.getIterator();
    while (it.next()) {
      for (int i = 0; i < columnCount; i++) {
        preparedStatement.setString(i + 1, it.get(i).toString());
      }
      preparedStatement.executeUpdate();
    }
    connection.close();
  }

  private void saveResult2Minio(DataSet result) {
    if (outputDataItem == null) {
      return;
    }
    String bucket = "result";
    String jobID = wxy_configFile.jobID;
    String dataID = outputDataItem.getDataID();
    String objectName = jobID + "/" + dataID;
    Schema schema = result.getSchema();
    int columnCount = schema.getColumnDescs().size();
    try (FileWriter csvWriter = new FileWriter("data.csv")) {
      StringBuilder sb = new StringBuilder();
      for (OpenHuFuData.ColumnDesc columnDesc : schema.getColumnDescs()) {
        sb.append(columnDesc.getName());
        sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.append("\n");
      DataSetIterator it = result.getIterator();
      while (it.next()) {
        for (int i = 0; i < columnCount; i++) {
          sb.append(it.get(i).toString());
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("\n");
      }
      csvWriter.write(sb.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
