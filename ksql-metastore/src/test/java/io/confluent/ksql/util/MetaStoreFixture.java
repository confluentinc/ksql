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

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(functionRegistry, new KsqlJsonSerdeFactory());
  }

  public static MutableMetaStore getNewMetaStore(
      final FunctionRegistry functionRegistry,
      final KsqlSerdeFactory valueSerdeFactory
  ) {
    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();

    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final Schema test1Schema = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("COL5", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .build();

    final KsqlTopic
        ksqlTopic0 =
        new KsqlTopic("TEST0", "test0", valueSerdeFactory, false);

    final KsqlStream<?> ksqlStream0 = new KsqlStream<>(
        "sqlexpression",
        "TEST0",
        LogicalSchema.of(test1Schema).withImplicitAndKeyFieldsInValue(),
        SerdeOption.none(),
        KeyField.of("COL0", test1Schema.field("COL0")),
        timestampExtractionPolicy,
        ksqlTopic0,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopic0);
    metaStore.putSource(ksqlStream0);

    final KsqlTopic
        ksqlTopic1 =
        new KsqlTopic("TEST1", "test1", valueSerdeFactory, false);

    final KsqlStream<?> ksqlStream1 = new KsqlStream<>("sqlexpression",
        "TEST1",
        LogicalSchema.of(test1Schema).withImplicitAndKeyFieldsInValue(),
        SerdeOption.none(),
        KeyField.of("COL0", test1Schema.field("COL0")),
        timestampExtractionPolicy,
        ksqlTopic1,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopic1);
    metaStore.putSource(ksqlStream1);

    final Schema test2Schema = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    final KsqlTopic
        ksqlTopic2 =
        new KsqlTopic("TEST2", "test2", valueSerdeFactory, false);
    final KsqlTable<String> ksqlTable = new KsqlTable<>(
        "sqlexpression",
        "TEST2",
        LogicalSchema.of(test2Schema),
        SerdeOption.none(),
        KeyField.of("COL0", test2Schema.field("COL0")),
        timestampExtractionPolicy,
        ksqlTopic2,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopic2);
    metaStore.putSource(ksqlTable);

    final Schema addressSchema = SchemaBuilder.struct()
        .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
        .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();

    final Schema categorySchema = SchemaBuilder.struct()
        .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    final Schema itemInfoSchema = SchemaBuilder.struct()
        .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CATEGORY", categorySchema)
        .optional().build();

    final Schema ordersSchema = SchemaBuilder.struct()
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", Schema.OPTIONAL_INT32_SCHEMA)
        .field("ARRAYCOL",SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("ADDRESS", addressSchema)
        .optional().build();

    final KsqlTopic
        ksqlTopicOrders =
        new KsqlTopic("ORDERS_TOPIC", "orders_topic", valueSerdeFactory, false);

    final KsqlStream<?> ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        "ORDERS",
        LogicalSchema.of(ordersSchema).withImplicitAndKeyFieldsInValue(),
        SerdeOption.none(),
        KeyField.of("ORDERTIME", ordersSchema.field("ORDERTIME")),
        timestampExtractionPolicy,
        ksqlTopicOrders,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    final Schema testTable3 = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    final KsqlTopic
        ksqlTopic3 =
        new KsqlTopic("TEST3", "test3", valueSerdeFactory, false);
    final KsqlTable<String> ksqlTable3 = new KsqlTable<>(
        "sqlexpression",
        "TEST3",
        LogicalSchema.of(testTable3),
        SerdeOption.none(),
        KeyField.of("COL0", testTable3.field("COL0")),
        timestampExtractionPolicy,
        ksqlTopic3,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopic3);
    metaStore.putSource(ksqlTable3);

    final Schema nestedArrayStructMapSchema = SchemaBuilder.struct()
        .field("ARRAYCOL", SchemaBuilder.array(itemInfoSchema).optional().build())
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, itemInfoSchema).optional().build())
        .field("NESTED_ORDER_COL", ordersSchema)
        .field("ITEM", itemInfoSchema)
        .optional().build();

    final KsqlTopic
        nestedArrayStructMapTopic =
        new KsqlTopic("NestedArrayStructMap", "NestedArrayStructMap_topic", valueSerdeFactory,
            false);

    final KsqlStream<?> nestedArrayStructMapOrders = new KsqlStream<>(
        "sqlexpression",
        "NESTED_STREAM",
        LogicalSchema.of(nestedArrayStructMapSchema).withImplicitAndKeyFieldsInValue(),
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        nestedArrayStructMapTopic,
        Serdes::String
    );

    metaStore.putTopic(nestedArrayStructMapTopic);
    metaStore.putSource(nestedArrayStructMapOrders);

    final KsqlTopic ksqlTopic4 =
        new KsqlTopic("TEST4", "test4", valueSerdeFactory, false);

    final KsqlStream<?> ksqlStream4 = new KsqlStream<>(
        "sqlexpression4",
        "TEST4",
        LogicalSchema.of(test1Schema),
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        ksqlTopic4,
        Serdes::String
    );

    metaStore.putTopic(ksqlTopic4);
    metaStore.putSource(ksqlStream4);

    return metaStore;
  }
}
