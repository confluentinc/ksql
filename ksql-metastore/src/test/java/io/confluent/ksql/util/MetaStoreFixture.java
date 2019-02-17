/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(functionRegistry, KsqlJsonTopicSerDe::new);
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry,
                                          final Supplier<KsqlTopicSerDe> serde) {

    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("COL5", SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).optional().build());

    final KsqlTopic
        ksqlTopic1 =
        new KsqlTopic("TEST1", "test1", serde.get(), false);

    final KsqlStream ksqlStream = new KsqlStream<>("sqlexpression",
        "TEST1",
        schemaBuilder1,
        schemaBuilder1.field("COL0"),
        timestampExtractionPolicy,
        ksqlTopic1,
        Serdes.String());

    metaStore.putTopic(ksqlTopic1);
    metaStore.putSource(ksqlStream);

    final SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);

    final KsqlTopic
        ksqlTopic2 =
        new KsqlTopic("TEST2", "test2", serde.get(), false);
    final KsqlTable<String> ksqlTable = new KsqlTable<>(
        "sqlexpression",
        "TEST2",
        schemaBuilder2,
        schemaBuilder2.field("COL0"),
        timestampExtractionPolicy,
        ksqlTopic2,
        "TEST2",
        Serdes.String());

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

    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final Schema schemaBuilderOrders = schemaBuilder
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", Schema.INT32_SCHEMA)
        .field("ARRAYCOL",SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("ADDRESS", addressSchema)
        .optional().build();

    final KsqlTopic
        ksqlTopicOrders =
        new KsqlTopic("ORDERS_TOPIC", "orders_topic", serde.get(), false);

    final KsqlStream ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        "ORDERS",
        schemaBuilderOrders,
        schemaBuilderOrders.field("ORDERTIME"),
        timestampExtractionPolicy,
        ksqlTopicOrders,
        Serdes.String());

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    final SchemaBuilder schemaBuilderTestTable3 = SchemaBuilder.struct()
        .field("ROWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);

    final KsqlTopic
        ksqlTopic3 =
        new KsqlTopic("TEST3", "test3", serde.get(), false);
    final KsqlTable<String> ksqlTable3 = new KsqlTable<>(
        "sqlexpression",
        "TEST3",
        schemaBuilderTestTable3,
        schemaBuilderTestTable3.field("COL0"),
        timestampExtractionPolicy,
        ksqlTopic3,
        "TEST3",
        Serdes.String());

    metaStore.putTopic(ksqlTopic3);
    metaStore.putSource(ksqlTable3);

    final Schema nestedArrayStructMapSchema = SchemaBuilder.struct()
        .field("ARRAYCOL", SchemaBuilder.array(itemInfoSchema))
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, itemInfoSchema))
        .field("NESTED_ORDER_COL", schemaBuilderOrders)
        .field("ITEM", itemInfoSchema)
        .optional().build();

    final KsqlTopic
        nestedArrayStructMapTopic =
        new KsqlTopic("NestedArrayStructMap", "NestedArrayStructMap_topic", serde.get(), false);

    final KsqlStream nestedArrayStructMapOrders = new KsqlStream<>(
        "sqlexpression",
        "NESTED_STREAM",
        nestedArrayStructMapSchema,
        null,
        timestampExtractionPolicy,
        nestedArrayStructMapTopic,
        Serdes.String());

    metaStore.putTopic(nestedArrayStructMapTopic);
    metaStore.putSource(nestedArrayStructMapOrders);

    return metaStore;
  }
}
