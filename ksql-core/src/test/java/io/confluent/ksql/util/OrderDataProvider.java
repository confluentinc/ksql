/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OrderDataProvider extends TestDataProvider {

  private static final String namePrefix =
      "ORDER";

  private static final String ksqlSchemaString =
      "(ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>)";

  private static final String key = "ORDERTIME";

  private static final Schema schema = SchemaBuilder.struct()
      .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
      .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
      .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
      .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA)
      .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
      .field("KEYVALUEMAP", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.FLOAT64_SCHEMA)).build();

  private static final Map<String, GenericRow> data = buildData();

  public OrderDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private static Map<String, GenericRow> buildData() {

    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);

    Map<String, GenericRow> dataMap = new HashMap<>();
    dataMap.put("1", new GenericRow(Arrays.asList(1,
        "ORDER_1",
        "ITEM_1", 10.0, new
            Double[]{100.0,
            110.99,
            90.0 },
        mapField)));
    dataMap.put("2", new GenericRow(Arrays.asList(2, "ORDER_2",
        "ITEM_2", 20.0, new
            Double[]{10.0,
            10.99,
            9.0 },
        mapField)));

    dataMap.put("3", new GenericRow(Arrays.asList(3, "ORDER_3",
        "ITEM_3", 30.0, new
            Double[]{10.0,
            10.99,
            91.0 },
        mapField)));

    dataMap.put("4", new GenericRow(Arrays.asList(4, "ORDER_4",
        "ITEM_4", 40.0, new
            Double[]{10.0,
            140.99,
            94.0 },
        mapField)));

    dataMap.put("5", new GenericRow(Arrays.asList(5, "ORDER_5",
        "ITEM_5", 50.0, new
            Double[]{160.0,
            160.99,
            98.0 },
        mapField)));

    dataMap.put("6", new GenericRow(Arrays.asList(6, "ORDER_6",
        "ITEM_6", 60.0, new
            Double[]{1000.0,
            1100.99,
            900.0 },
        mapField)));

    dataMap.put("7", new GenericRow(Arrays.asList(7, "ORDER_6",
        "ITEM_7", 70.0, new
            Double[]{1100.0,
            1110.99,
            190.0 },
        mapField)));

    dataMap.put("8", new GenericRow(Arrays.asList(8, "ORDER_6",
        "ITEM_8", 80.0, new
            Double[]{1100.0,
            1110.99,
            970.0 },
        mapField)));

    return dataMap;
  }

}
