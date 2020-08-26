/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOptions;
import java.util.Map;

public class OrderDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ORDERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("TIMESTAMP"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("PRICEARRAY"), SqlTypes.array(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("KEYVALUEMAP"), SqlTypes.map(SqlTypes.DOUBLE))
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOptions.of());

  private static final Map<String, Double> MAP_FIELD = ImmutableMap.of(
      "key1", 1.0,
      "key2", 2.0,
      "key3", 3.0
  );

  private static final Multimap<String, GenericRow> ROWS = ImmutableListMultimap
      .<String, GenericRow>builder()
      .put(
          "ORDER_1",
          genericRow(
              1L,
              "ITEM_1",
              10.0,
              "2018-01-01",
              ImmutableList.of(100.0, 110.99, 90.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_2",
          genericRow(
              2L,
              "ITEM_2",
              20.0,
              "2018-01-02",
              ImmutableList.of(10.0, 10.99, 9.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_3",
          genericRow(
              3L,
              "ITEM_3",
              30.0,
              "2018-01-03",
              ImmutableList.of(10.0, 10.99, 91.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_4",
          genericRow(
              4L,
              "ITEM_4",
              40.0,
              "2018-01-04",
              ImmutableList.of(10.0, 140.99, 94.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_5",
          genericRow(
              5L,
              "ITEM_5",
              50.0,
              "2018-01-05",
              ImmutableList.of(160.0, 160.99, 98.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_6",
          genericRow(
              6L,
              "ITEM_6",
              60.0,
              "2018-01-06",
              ImmutableList.of(1000.0, 1100.99, 900.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_6",
          genericRow(
              7L,
              "ITEM_7",
              70.0,
              "2018-01-07",
              ImmutableList.of(1100.0, 1110.99, 190.0),
              MAP_FIELD
          ))
      .put(
          "ORDER_6",
          genericRow(
              8L,
              "ITEM_8",
              80.0,
              "2018-01-08",
              ImmutableList.of(1100.0, 1110.99, 970.0),
              MAP_FIELD
          ))
      .build();

  public OrderDataProvider() {
    super("ORDER", PHYSICAL_SCHEMA, ROWS);
  }
}