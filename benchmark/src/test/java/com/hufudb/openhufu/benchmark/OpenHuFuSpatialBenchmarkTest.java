package com.hufudb.openhufu.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.hufudb.openhufu.benchmark.enums.SpatialTableName;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.user.OpenHuFuUser;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OpenHuFuSpatialBenchmarkTest {
  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuBenchmark.class);
  private static final OpenHuFuUser user = new OpenHuFuUser();

  @BeforeClass
  public static void setUp() throws IOException {
    LinkedTreeMap userConfigs = new Gson().fromJson(Files.newBufferedReader(
                    Path.of(OpenHuFuBenchmark.class.getClassLoader().getResource("spatial-user-configs.json")
                            .getPath())),
            LinkedTreeMap.class);
    List<String> endpoints = (List<String>) userConfigs.get("owners");
    List<GlobalTableConfig> globalTableConfigs = new Gson().fromJson(new Gson().toJson(userConfigs.get("tables")),
            new TypeToken<ArrayList<GlobalTableConfig>>() {}.getType());
    LOG.info("Init benchmark of OpenHuFuSpatial...");
    for (String endpoint : endpoints) {
      user.addOwner(endpoint, null);
    }

    for (GlobalTableConfig config : globalTableConfigs) {
      user.createOpenHuFuTable(config);
    }
    LOG.info("Init finish");
  }

  public void printLine(ResultSet it) throws SQLException {
    for (int i = 1; i <= it.getMetaData().getColumnCount(); i++) {
      System.out.print(it.getString(i) + "|");
    }
    System.out.println();
  }

  @Test
  public void testSqlSelect() throws SQLException {
    String sql = "select * from spatial";
    ResultSet dataset = user.executeQuery(sql);
    int count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(3000, count);
    dataset.close();
  }

  @Test
  public void testSqlSpatialDistance() throws SQLException {
    String sql = "select Distance(S_POINT, POINT(1404050, -4762163)) from spatial";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(3000, count);
    dataset.close();
  }

  @Test
  public void testSqlRangeQuery() throws SQLException {
    String sql = "select * from spatial where DWithin(POINT(1404050, -4762163), S_POINT, 5)";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(1, count);
    dataset.close();
  }

  @Test
  public void testSqlRangeCount() throws SQLException {
    String sql = "select count(*) from spatial where DWithin(POINT(1404050, -4762163), S_POINT, 5)";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(1, count);
    dataset.close();
  }

  @Test
  public void testSqlRangeJoin() throws SQLException {
    String sql = "select * from spatial s1 join spatial s2 on DWithin(s1.S_POINT, s2.S_POINT, 5)";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(1, count);
    dataset.close();
  }

  @Test
  public void testSqlKNNQuery() throws SQLException {
    String sql = "select * from spatial order by Distance(S_POINT, POINT(1404050, -4762163)) asc limit 10";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(1, count);
    dataset.close();
  }

  @Test
  public void testSqlKNNJOIN() throws SQLException {
    String sql = "select * from spatial s1 join spatial s2 on KNN(s1.S_POINT, s2.S_POINT, 5)";
    ResultSet dataset = user.executeQuery(sql);
    long count = 0;
    while (dataset.next()) {
      printLine(dataset);
      ++count;
    }
    assertEquals(1, count);
    dataset.close();
  }

  @Test
  public void testSelect() {
    String tableName = SpatialTableName.SPATIAL.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);
    plan.setSelectExps(ExpressionFactory
        .createInputRef(user.getOpenHuFuTableSchema(tableName).getSchema()));
    DataSet dataset = user.executeQuery(plan);
    DataSetIterator it = dataset.getIterator();
    long count = 0;
    while (it.next()) {
      for (int i = 0; i < it.size(); i++) {
        System.out.print(it.get(i) + "|");
      }
      System.out.println();
      ++count;
    }
    assertEquals(3000, count);
    dataset.close();
  }

  @Test
  public void testSpatialDistance() {
    String tableName = SpatialTableName.SPATIAL.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);

    // select Distance(S_POINT, POINT((1404050.076199729, -4762163.267865509)) from spatial;
    Expression pointFunc =
        ExpressionFactory.createLiteral(ColumnType.GEOMETRY, GeometryUtils.fromString("POINT(1404050.076199729 -4762163.267865509)"));
    Expression distanceFunc =
        ExpressionFactory.createScalarFunc(ColumnType.DOUBLE, "Distance",
            ImmutableList.of(pointFunc, pointFunc));
    plan.setSelectExps(ImmutableList.of(distanceFunc));
    DataSet dataset = user.executeQuery(plan);
    DataSetIterator it = dataset.getIterator();
    int count = 0;
    assertEquals(1, it.size());
    while (it.next()) {
      assertEquals(0.0, it.get(0));
      count++;
    }
    assertEquals(3000, count);
  }

  @Test
  public void testSpatialDWithin() {
    String tableName = SpatialTableName.SPATIAL.getName();
    LeafPlan plan = new LeafPlan();
    plan.setTableName(tableName);
    plan.setSelectExps(
        ExpressionFactory.createInputRef(user.getOpenHuFuTableSchema(tableName).getSchema()));
    // select * from spatial where DWithin(S_POINT, POINT((1404050.076199729, -4762163.267865509), 0.1);
    Expression pointFunc =
    ExpressionFactory.createLiteral(ColumnType.GEOMETRY, GeometryUtils.fromString("POINT(1404050.076199729 -4762163.267865509)"));
    Expression dwithinFunc =
        ExpressionFactory.createScalarFunc(ColumnType.BOOLEAN, "DWithin",
            ImmutableList.of(
                ExpressionFactory.createInputRef(1, ColumnType.GEOMETRY, Modifier.PUBLIC),
                pointFunc, ExpressionFactory.createLiteral(ColumnType.DOUBLE, 0.1)));
    plan.setWhereExps(ImmutableList.of(dwithinFunc));
    DataSet dataset = user.executeQuery(plan);
    DataSetIterator it = dataset.getIterator();
    int count = 0;
    assertEquals(2, it.size());
    while (it.next()) {
      assertEquals(0L, it.get(0));
      count++;
    }
    assertEquals(1, count);
  }
}
