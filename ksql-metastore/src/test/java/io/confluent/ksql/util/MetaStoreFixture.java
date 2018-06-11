/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;

public class MetaStoreFixture {

  public static MetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {

    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();
    final MetaStore metaStore = new MetaStoreImpl(functionRegistry);

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("COL5", SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).optional().build());

    KsqlTopic
        ksqlTopic1 =
        new KsqlTopic("TEST1", "test1", new KsqlJsonTopicSerDe());

    KsqlStream ksqlStream = new KsqlStream("sqlexpression",
        "TEST1",
        schemaBuilder1,
        schemaBuilder1.field("COL0"),
        timestampExtractionPolicy,
        ksqlTopic1);

    metaStore.putTopic(ksqlTopic1);
    metaStore.putSource(ksqlStream);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);

    KsqlTopic
        ksqlTopic2 =
        new KsqlTopic("TEST2", "test2", new KsqlJsonTopicSerDe());
    KsqlTable ksqlTable = new KsqlTable(
        "sqlexpression",
        "TEST2",
        schemaBuilder2,
        schemaBuilder2.field("COL0"),
        timestampExtractionPolicy,
        ksqlTopic2,
        "TEST2",
        false);

    metaStore.putTopic(ksqlTopic2);
    metaStore.putSource(ksqlTable);

    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
        .field("ORDERTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("ITEMID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA);

    KsqlTopic
        ksqlTopicOrders =
        new KsqlTopic("ORDERS_TOPIC", "orders_topic", new KsqlJsonTopicSerDe());

    KsqlStream ksqlStreamOrders = new KsqlStream(
        "sqlexpression",
        "ORDERS",
        schemaBuilderOrders,
        schemaBuilderOrders.field("ORDERTIME"),
        timestampExtractionPolicy,
        ksqlTopicOrders);

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    return metaStore;
  }
}