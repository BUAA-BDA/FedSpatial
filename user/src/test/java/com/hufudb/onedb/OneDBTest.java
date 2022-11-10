package com.hufudb.onedb;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.user.OneDB;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.net.URL;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneDBTest {

  static OneDB oneDB;
  static int oneDBTableNumber;

  @Before
  public void setUp() {
    oneDB = new OneDB();
    ClassLoader classLoader = OneDBTest.class.getClassLoader();
    URL resource = classLoader.getResource("ci_crt/ca.pem");
    List<String> endpoints = ImmutableList.of("owner1:12345", "owner2:12345", "owner3:12345");
    endpoints.forEach(e -> {
      assert resource != null;
      assertTrue(oneDB.addOwner(e, resource.getPath()));
    });
    // Note: if add new OneDBTable, change the oneDBTableNumber below
    oneDB.createOneDBTable(new GlobalTableConfig("region",
            Stream.of(endpoints.get(0)).map(e -> new LocalTableConfig(e, "region"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("nation",
            Stream.of(endpoints.get(0)).map(e -> new LocalTableConfig(e, "nation"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("partsupp",
            endpoints.stream().map(e -> new LocalTableConfig(e, "partsupp"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("customer",
            endpoints.stream().map(e -> new LocalTableConfig(e, "customer"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("lineitem",
            endpoints.stream().map(e -> new LocalTableConfig(e, "lineitem"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("orders",
            endpoints.stream().map(e -> new LocalTableConfig(e, "orders"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("part",
            endpoints.stream().map(e -> new LocalTableConfig(e, "part"))
                    .collect(Collectors.toList())));
    oneDB.createOneDBTable(new GlobalTableConfig("supplier",
            endpoints.stream().map(e -> new LocalTableConfig(e, "supplier"))
                    .collect(Collectors.toList())));
    oneDBTableNumber = 8;
  }

  @Test
  public void testOwnerOperations() {
    Set<String> endpoints = oneDB.getEndpoints();
    assertTrue(endpoints.contains("owner1:12345"));
    assertTrue(endpoints.contains("owner2:12345"));
    assertTrue(endpoints.contains("owner3:12345"));

    assertTrue(oneDB.getOwnerTableSchema("owner1:12345").size() > 0);
    assertEquals(oneDBTableNumber, oneDB.getAllOneDBTableSchema().size());
    
    assertTrue("Error when add a existing owner", oneDB.addOwner("owner1:12345"));
    oneDB.removeOwner("owner1:12345");

    assertFalse("Error when add a existing global table", oneDB.createOneDBTable(new GlobalTableConfig("region", ImmutableList.of(new LocalTableConfig("owner1:12345", "region")))));
    oneDB.dropOneDBTable("region");
    assertEquals(ImmutableList.of(), oneDB.getOwnerTableSchema("region"));
    assertNull(oneDB.getOneDBTableSchema("region"));
    oneDB.close();
  }

  @Test
  public void testProject() {
    ResultSet rs = oneDB.executeQuery("select l_shipmode, " +
            "l_extendedprice * (1 - l_discount) * (1 + l_tax), " +
            "l_extendedprice * (1 - l_discount) * (1 + l_tax), " +
            "l_quantity " +
            "from lineitem " +
            "where  l_shipdate <= 755366400 and l_extendedprice * l_tax >= 230");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql1.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testCaseWhen() {
    ResultSet rs = oneDB.executeQuery("select case  when o_orderpriority = '1-URGENT' " +
            "or o_orderpriority = '2-HIGH'  then 1  else 0  end " +
            "from  orders  where  " +
            "o_totalprice * o_totalprice / o_totalprice < 90000");
    List<ColumnType> header = Collections.singletonList(ColumnType.INT);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql2.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testDistinct() {
    ResultSet rs = oneDB.executeQuery("select distinct l_orderkey " +
            "from  lineitem where   " +
            "l_commitdate < l_receiptdate or l_discount > l_tax");
    List<ColumnType> header = Collections.singletonList(ColumnType.INT);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql3.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testAggregate() {
    ResultSet rs = oneDB.executeQuery("select  sum(l_quantity),  sum(l_extendedprice),  " +
            "sum(l_extendedprice * (1 - l_discount)),  " +
            "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),  " +
            "avg(l_extendedprice / (l_extendedprice * (1 - l_discount) * (1 + l_tax))),  " +
            "max(l_extendedprice) * min(l_extendedprice * (1 - l_discount)),  " +
            "count(*) from  lineitem");
    List<ColumnType> header = Arrays.asList(ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql4.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testGroupBy() {
    ResultSet rs = oneDB.executeQuery("select  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty, " +
            " sum(l_extendedprice) as sum_base_price, " +
            " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,  " +
            "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  " +
            "avg(l_quantity) as avg_qty,  avg(l_extendedprice) as avg_price,  " +
            "avg(l_discount) as avg_disc,  count(*) as count_order " +
            "from  lineitem " +
            "group by  l_returnflag,  l_linestatus");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.STRING ,ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE,
            ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.LONG);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql5.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }

  @Test
  public void testSortLimit() {
    ResultSet rs = oneDB.executeQuery("select l_orderkey, l_extendedprice " +
            "from  lineitem " +
            "order by  l_shipmode ASC,  " +
            "l_shipinstruct DESC,  " +
            "l_extendedprice * l_discount / l_quantity ASC , l_orderkey " +
            "limit 100 offset 20");
    List<ColumnType> header = Arrays.asList(ColumnType.INT, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql6.csv", header);
    assert resultDataSet.compareWithOrder();
  }

  @Test
  public void testJoin() {
    ResultSet rs = oneDB.executeQuery("select  c_name,  c_custkey,  o_orderkey,  " +
            "o_orderdate,  o_totalprice " +
            "from  customer,  orders,  lineitem " +
            "where  c_custkey = o_custkey  and o_orderkey = l_orderkey");
    List<ColumnType> header = Arrays.asList(ColumnType.STRING, ColumnType.INT, ColumnType.INT,
            ColumnType.INT, ColumnType.DOUBLE);
    ResultDataSet resultDataSet = new ResultDataSet();
    resultDataSet.addOutput(rs);
    resultDataSet.addRealAnswer("sql7.csv", header);
    assert resultDataSet.compareWithoutOrder();
  }
}