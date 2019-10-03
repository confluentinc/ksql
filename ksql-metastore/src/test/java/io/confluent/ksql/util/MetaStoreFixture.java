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
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(functionRegistry, ValueFormat.of(FormatInfo.of(Format.JSON)));
  }

  public static MutableMetaStore getNewMetaStore(
      final FunctionRegistry functionRegistry,
      final ValueFormat valueFormat
  ) {
    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();

    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));

    final LogicalSchema test1Schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("COL5"), SqlTypes.map(SqlTypes.DOUBLE))
        .build();

    final KsqlTopic ksqlTopic0 = new KsqlTopic(
        "test0",
        keyFormat,
        valueFormat,
        false
    );

    final KsqlStream<?> ksqlStream0 = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("TEST0"),
        test1Schema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL0")), test1Schema.findValueColumn(
            ColumnRef.withoutSource(ColumnName.of("COL0"))).get()),
        timestampExtractionPolicy,
        ksqlTopic0
    );

    metaStore.putSource(ksqlStream0);

    final KsqlTopic ksqlTopic1 = new KsqlTopic(
        "test1",
        keyFormat,
        valueFormat,
        false
    );

    final KsqlStream<?> ksqlStream1 = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("TEST1"),
        test1Schema,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0")),
            test1Schema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("COL0"))).get()),
        timestampExtractionPolicy,
        ksqlTopic1
    );

    metaStore.putSource(ksqlStream1);

    final LogicalSchema test2Schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KsqlTopic ksqlTopic2 = new KsqlTopic(
        "test2",
        keyFormat,
        valueFormat,
        false
    );
    final KsqlTable<String> ksqlTable = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TEST2"),
        test2Schema,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0")),
            test2Schema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("COL0"))).get()),
        timestampExtractionPolicy,
        ksqlTopic2
    );

    metaStore.putSource(ksqlTable);

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
        .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ITEMINFO"), itemInfoSchema)
        .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("ADDRESS"), addressSchema)
        .build();

    final KsqlTopic ksqlTopicOrders = new KsqlTopic(
        "orders_topic",
        keyFormat,
        valueFormat,
        false
    );

    final KsqlStream<?> ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("ORDERS"),
        ordersSchema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("ORDERTIME")),
            ordersSchema.findValueColumn(ColumnRef.withoutSource(ColumnName.of("ORDERTIME"))).get()),
        timestampExtractionPolicy,
        ksqlTopicOrders
    );

    metaStore.putSource(ksqlStreamOrders);

    final LogicalSchema testTable3 = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KsqlTopic ksqlTopic3 = new KsqlTopic(
        "test3",
        keyFormat,
        valueFormat,
        false
    );
    final KsqlTable<String> ksqlTable3 = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TEST3"),
        testTable3,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0")),
            testTable3.findValueColumn(ColumnRef.withoutSource(ColumnName.of("COL0"))).get()),
        timestampExtractionPolicy,
        ksqlTopic3
    );

    metaStore.putSource(ksqlTable3);

    final SqlType nestedOrdersSchema = SqlTypes.struct()
        .field("ORDERTIME", SqlTypes.BIGINT)
        .field("ORDERID", SqlTypes.BIGINT)
        .field("ITEMID", SqlTypes.STRING)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", SqlTypes.INTEGER)
        .field("ARRAYCOL", SqlTypes.array(SqlTypes.DOUBLE))
        .field("MAPCOL", SqlTypes.map(SqlTypes.DOUBLE))
        .field("ADDRESS", addressSchema)
        .build();

    final LogicalSchema nestedArrayStructMapSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(itemInfoSchema))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(itemInfoSchema))
        .valueColumn(ColumnName.of("NESTED_ORDER_COL"), nestedOrdersSchema)
        .valueColumn(ColumnName.of("ITEM"), itemInfoSchema)
        .build();

    final KsqlTopic nestedArrayStructMapTopic = new KsqlTopic(
        "NestedArrayStructMap_topic",
        keyFormat,
        valueFormat,
        false
    );

    final KsqlStream<?> nestedArrayStructMapOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("NESTED_STREAM"),
        nestedArrayStructMapSchema,
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        nestedArrayStructMapTopic
    );

    metaStore.putSource(nestedArrayStructMapOrders);

    final KsqlTopic ksqlTopic4 = new KsqlTopic(
        "test4",
        keyFormat,
        valueFormat,
        false
    );

    final KsqlStream<?> ksqlStream4 = new KsqlStream<>(
        "sqlexpression4",
        SourceName.of("TEST4"),
        test1Schema,
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        ksqlTopic4
    );

    metaStore.putSource(ksqlStream4);

    return metaStore;
  }
}
