package com.hufudb.onedb.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.Map;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.storage.Point;

public class UDFLoaderTest {
  @Test
  public void testLoadScalarUDF() {
    Map<String, ScalarUDF> udfs = UDFLoader.scalarUDFs;
    Point p1 = (Point) udfs.get("point").implement(ImmutableList.of(1.0, 2.0));
    Point p2 = (Point) udfs.get("point").implement(ImmutableList.of(1.0, 1.0));
    double distance = (double) udfs.get("distance").implement(ImmutableList.of(p1, p2));
    assertEquals(distance, 1.0, 0.001);
    assertTrue((boolean) udfs.get("dwithin").implement(ImmutableList.of(p1, p2, 1.00)));
    assertFalse((boolean) udfs.get("dwithin").implement(ImmutableList.of(p1, p2, 0.5)));
  }
}
