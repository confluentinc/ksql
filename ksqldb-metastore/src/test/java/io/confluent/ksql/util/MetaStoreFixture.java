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

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Optional;

public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(
        functionRegistry,
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
    );
  }

  public static MutableMetaStore getNewMetaStore(
      final FunctionRegistry functionRegistry,
      final ValueFormat valueFormat
  ) {
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final KeyFormat keyFormat = KeyFormat
        .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of());

    final LogicalSchema test1Schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("COL5"), SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE))
        .headerColumn(ColumnName.of("HEAD"), Optional.empty())
        .build();

    final KsqlTopic ksqlTopic0 = new KsqlTopic(
        "test0",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> ksqlStream0 = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("TEST0"),
        test1Schema,
        Optional.empty(),
        false, 
        ksqlTopic0,
        false
    );

    metaStore.putSource(ksqlStream0, false);

    final KsqlTopic ksqlTopic1 = new KsqlTopic(
        "test1",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> ksqlStream1 = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("TEST1"),
        test1Schema,
        Optional.empty(),
        false,
        ksqlTopic1,
        false
    );

    metaStore.putSource(ksqlStream1, false);

    final LogicalSchema test2Schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KsqlTopic ksqlTopic2 = new KsqlTopic(
        "test2",
        keyFormat,
        valueFormat
    );
    final KsqlTable<String> ksqlTable = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TEST2"),
        test2Schema,
        Optional.empty(),
        false,
        ksqlTopic2,
        false
    );

    metaStore.putSource(ksqlTable, false);

    final SqlType addressSchema = SqlTypes.struct()
        .field("NUMBER", SqlTypes.BIGINT)
        .field("STREET", SqlTypes.STRING)
        .field("CITY", SqlTypes.STRING)
        .field("STATE", SqlTypes.STRING)
        .field("ZIPCODE", SqlTypes.BIGINT)
        .build();

    final SqlType categorySchema = SqlTypes.struct()
        .field("ID", SqlTypes.BIGINT)
        .field("NAME", SqlTypes.STRING)
        .build();

    final SqlType itemInfoSchema = SqlTypes.struct()
        .field("ITEMID", SqlTypes.BIGINT)
        .field("NAME", SqlTypes.STRING)
        .field("CATEGORY", categorySchema)
        .build();

    final LogicalSchema ordersSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ITEMINFO"), itemInfoSchema)
        .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("ADDRESS"), addressSchema)
        .valueColumn(ColumnName.of("TIMESTAMPCOL"), SqlTypes.TIMESTAMP)
        .valueColumn(ColumnName.of("TIMECOL"), SqlTypes.TIME)
        .valueColumn(ColumnName.of("DATECOL"), SqlTypes.DATE)
        .valueColumn(ColumnName.of("BYTESCOL"), SqlTypes.BYTES)
        .build();

    final KsqlTopic ksqlTopicOrders = new KsqlTopic(
        "orders_topic",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("ORDERS"),
        ordersSchema,
        Optional.empty(),
        false,
        ksqlTopicOrders,
        false
    );

    metaStore.putSource(ksqlStreamOrders, false);

    final LogicalSchema testTable3 = LogicalSchema.builder()
        .keyColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KsqlTopic ksqlTopic3 = new KsqlTopic(
        "test3",
        keyFormat,
        valueFormat
    );
    final KsqlTable<String> ksqlTable3 = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TEST3"),
        testTable3,
        Optional.empty(),
        false,
        ksqlTopic3,
        false
    );

    metaStore.putSource(ksqlTable3, false);

    final SqlType nestedOrdersSchema = SqlTypes.struct()
        .field("ORDERTIME", SqlTypes.BIGINT)
        .field("ORDERID", SqlTypes.BIGINT)
        .field("ITEMID", SqlTypes.STRING)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", SqlTypes.INTEGER)
        .field("ARRAYCOL", SqlTypes.array(SqlTypes.DOUBLE))
        .field("MAPCOL", SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE))
        .field("ADDRESS", addressSchema)
        .build();

    final LogicalSchema nestedArrayStructMapSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(itemInfoSchema))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.STRING, itemInfoSchema))
        .valueColumn(ColumnName.of("NESTED_ORDER_COL"), nestedOrdersSchema)
        .valueColumn(ColumnName.of("ITEM"), itemInfoSchema)
        .build();

    final KsqlTopic nestedArrayStructMapTopic = new KsqlTopic(
        "NestedArrayStructMap_topic",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> nestedArrayStructMapOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("NESTED_STREAM"),
        nestedArrayStructMapSchema,
        Optional.empty(),
        false,
        nestedArrayStructMapTopic,
        false
    );

    metaStore.putSource(nestedArrayStructMapOrders, false);

    final KsqlTopic ksqlTopic4 = new KsqlTopic(
        "test4",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> ksqlStream4 = new KsqlStream<>(
        "sqlexpression4",
        SourceName.of("TEST4"),
        test1Schema,
        Optional.empty(),
        false,
        ksqlTopic4,
        false
    );

    metaStore.putSource(ksqlStream4, false);

    final LogicalSchema sensorReadingsSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("ID"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("SENSOR_NAME"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ARR1"), SqlTypes.array(SqlTypes.BIGINT))
        .valueColumn(ColumnName.of("ARR2"), SqlTypes.array(SqlTypes.STRING))
        .build();

    final KsqlTopic ksqlTopicSensorReadings = new KsqlTopic(
        "sensor_readings_topic",
        keyFormat,
        valueFormat
    );

    final KsqlStream<?> ksqlStreamSensorReadings = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("SENSOR_READINGS"),
        sensorReadingsSchema,
        Optional.empty(),
        false,
        ksqlTopicSensorReadings,
        false
    );

    metaStore.putSource(ksqlStreamSensorReadings, false);

    final LogicalSchema testTable5 = LogicalSchema.builder()
        .keyColumn(ColumnName.of("A"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("B"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("C"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("D"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("E"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("F"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("G"), SqlTypes.BOOLEAN)
        .build();

    final KsqlTopic ksqlTopic5 = new KsqlTopic(
        "test5",
        keyFormat,
        valueFormat
    );
    final KsqlTable<String> ksqlTable5 = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TEST5"),
        testTable5,
        Optional.empty(),
        false,
        ksqlTopic5,
        false
    );
    metaStore.putSource(ksqlTable5, false);

    return metaStore;
  }
}
