package com.hufudb.onedb.owner.adapter.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.ResultDataSet;
import com.hufudb.onedb.expression.Translator;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.proto.OneDBData;
import com.hufudb.onedb.proto.OneDBData.Desensitize;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.plan.Plan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base adapter for datasource with jdbc support
 */
public abstract class JDBCAdapter implements Adapter {
  protected final static Logger LOG = LoggerFactory.getLogger(JDBCAdapter.class);

  protected String catalog;
  protected Connection connection;
  protected Statement statement;
  protected final AdapterTypeConverter converter;
  protected final SchemaManager schemaManager;
  protected final JDBCTranslator translator;

  protected JDBCAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter, Translator translator) {
    this.catalog = catalog;
    this.connection = connection;
    this.statement = statement;
    this.converter = converter;
    this.schemaManager = new SchemaManager();
    this.translator = new JDBCTranslator(translator);
    loadAllTableSchema();
  }

  public void loadAllTableSchema() {
    try {
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet rs = meta.getTables(catalog, null, "%", new String[] {"TABLE"});
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        schemaManager.addLocalTable(getTableSchema(tableName, meta));
      }
      rs.close();
    } catch (Exception e) {
      LOG.error("Fail to load all table info: {}", e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public DataSet query(Plan queryPlan) {
    String sql = generateSQL(queryPlan);
    Schema schema = queryPlan.getOutSchema();
    if (!sql.isEmpty()) {
      return executeSQL(sql, schema);
    } else {
      return EmptyDataSet.INSTANCE;
    }
  }

  @Override
  public void init() {
    // do nothing
  }

  @Override
  public void shutdown() {
    try {
      statement.close();
      connection.close();
    } catch (Exception e) {
      LOG.error("Fail to close statement/connection: {}", e.getMessage());
    }
  }

  @Override
  public SchemaManager getSchemaManager() {
    return schemaManager;
  }

  protected TableSchema getTableSchema(String tableName, DatabaseMetaData meta) {
    try {
      ResultSet rc = meta.getColumns(catalog, null, tableName, null);
      TableSchema.Builder TableSchemaBuilder = TableSchema.newBuilder();
      TableSchemaBuilder.setTableName(tableName);
      while (rc.next()) {
        String columnName = rc.getString("COLUMN_NAME");
        TableSchemaBuilder.add(columnName, converter.convert(rc.getString("TYPE_NAME")));
      }
      rc.close();
      return TableSchemaBuilder.build();
    } catch (Exception e) {
      LOG.error("Error when load TableSchema of {}: ", tableName, e.getMessage());
      return null;
    }
  }

  protected String generateSQL(Plan plan) {
    assert plan.getPlanType().equals(PlanType.LEAF);
    String actualTableName = schemaManager.getActualTableName(plan.getTableName());
    Schema tableSchema = schemaManager.getActualSchema(plan.getTableName());
    LOG.info("Query {}: {}", actualTableName, tableSchema);
    final List<String> filters = translator.translateExps(tableSchema, plan.getWhereExps());
    final List<String> selects = translator.translateExps(tableSchema, plan.getSelectExps());
    final List<String> groups =
        plan.getGroups().stream().map(ref -> selects.get(ref)).collect(Collectors.toList());
    // order by
    List<String> order = translator.translateOrders(selects, plan.getOrders());
    StringBuilder sql = new StringBuilder();
    // select from clause
    if (!plan.getAggExps().isEmpty()) {
      final List<String> aggs = translator.translateAgg(selects, plan.getAggExps());
      sql.append(String.format("SELECT %s from %s", String.join(",", aggs), actualTableName));
    } else {
      sql.append(String.format("SELECT %s from %s", String.join(",", selects), actualTableName));
    }
    // where clause
    if (!filters.isEmpty()) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    if (!groups.isEmpty()) {
      sql.append(String.format(" group by %s", String.join(",", groups)));
    }
    if (!order.isEmpty()) {
      sql.append(String.format(" order by %s", String.join(",", order)));
    }
    if (plan.getFetch() != 0) {
      sql.append(" LIMIT ").append(plan.getFetch() + plan.getOffset());
    }
    return sql.toString();
  }

  protected DataSet executeSQL(String sql, Schema schema) {
    try {
      ResultSet rs = statement.executeQuery(sql);
      String table = rs.getMetaData().getTableName(1);
      Schema desensitizationSchema = desensitize(rs, schema, table);
      LOG.info("Execute {}", sql);
      return new ResultDataSet(schema, desensitizationSchema, rs);
    } catch (SQLException e) {
      LOG.error("Fail to execute SQL [{}]: {}", sql, e.getMessage());
      return EmptyDataSet.INSTANCE;
    }
  }

  public Schema desensitize(ResultSet rs, Schema schema, String tableName) throws SQLException {
    TableSchema desensitizationTable = schemaManager.getDesensitizationMap().get(tableName);
    if (desensitizationTable == null) {
      return schema;
    }
    ResultSetMetaData resultSetMetaData = rs.getMetaData();
    int colCount = resultSetMetaData.getColumnCount();
    List<OneDBData.ColumnDesc> columnDescs = new ArrayList<>();
    for (int i = 0; i < colCount; i++) {
      String colName = resultSetMetaData.getColumnName(i+1);
      Desensitize desensitize = desensitizationTable.getDesensitize(colName);
      OneDBData.ColumnType colType = schema.getType(i);
      OneDBData.Modifier modifier = schema.getModifier(i);
      columnDescs.add(OneDBData.ColumnDesc.newBuilder().setName(colName).setType(colType).setModifier(modifier).setDesensitize(desensitize).build());
    }
    return Schema.fromColumnDesc(columnDescs);
  }
}
