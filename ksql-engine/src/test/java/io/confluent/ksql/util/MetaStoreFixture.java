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

import io.confluent.ksql.metastore.*;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class MetaStoreFixture {

  public static MetaStore getNewMetaStore() {

    MetaStore metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilder1 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
        .field("COL5", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.FLOAT64_SCHEMA));

    KsqlTopic
        ksqlTopic1 =
        new KsqlTopic("TEST1", "test1", new KsqlJsonTopicSerDe(null));

    KsqlStream ksqlStream = new KsqlStream("TEST1", schemaBuilder1, schemaBuilder1.field("COL0"), null,
        ksqlTopic1);

    metaStore.putTopic(ksqlTopic1);
    metaStore.putSource(ksqlStream);

    SchemaBuilder schemaBuilder2 = SchemaBuilder.struct()
        .field("COL0", SchemaBuilder.INT64_SCHEMA)
        .field("COL1", SchemaBuilder.STRING_SCHEMA)
        .field("COL2", SchemaBuilder.STRING_SCHEMA)
        .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.BOOLEAN_SCHEMA);

    KsqlTopic
        ksqlTopic2 =
        new KsqlTopic("TEST2", "test2", new KsqlJsonTopicSerDe(null));
    KsqlTable ksqlTable = new KsqlTable("TEST2", schemaBuilder2, schemaBuilder2.field("COL0"),
                                        null,
        ksqlTopic2, "TEST2", false);

    metaStore.putTopic(ksqlTopic2);
    metaStore.putSource(ksqlTable);

    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
        .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
        .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
        .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA);

    KsqlTopic
        ksqlTopicOrders =
        new KsqlTopic("ORDERS_TOPIC", "orders_topic", new KsqlJsonTopicSerDe(null));

    KsqlStream ksqlStreamOrders = new KsqlStream("ORDERS", schemaBuilderOrders,
                                                 schemaBuilderOrders.field("ORDERTIME"), null,
        ksqlTopicOrders);

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    return metaStore;
  }


  public static void assertExpectedWindowedResults(Map<Windowed<String>, GenericRow> actualResult,
                                                Map<Windowed<String>, GenericRow> expectedResult) {
    Map<String, GenericRow> actualResultSimplified = new HashMap<>();
    Map<String, GenericRow> expectedResultSimplified = new HashMap<>();
    for (Windowed<String> k: expectedResult.keySet()) {
      expectedResultSimplified.put(k.key(), expectedResult.get(k));
    }

    for (Windowed<String> k: actualResult.keySet()) {
      if (actualResult.get(k) != null) {
        actualResultSimplified.put(k.key(), actualResult.get(k));
      }

    }
    assertThat(actualResultSimplified, equalTo(expectedResultSimplified));
  }

}