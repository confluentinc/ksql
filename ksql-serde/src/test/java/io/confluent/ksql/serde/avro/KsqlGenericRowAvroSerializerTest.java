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

package io.confluent.ksql.serde.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class KsqlGenericRowAvroSerializerTest {

  final Schema schema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(
            "arraycol".toUpperCase(),
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field(
            "mapcol".toUpperCase(),
            SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .optional()
        .build();

  @Test
  public void shouldSerializeRowCorrectly() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    final Serializer<GenericRow> serializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false, schemaRegistryClient
        ).serializer();

    final List columns = Arrays.asList(
        1511897796092L, 1L, "item_1", 10.0, Arrays.asList(100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    final byte[] serializedRow = serializer.serialize("t1", genericRow);
    final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord genericRecord =
        (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()).toString(), equalTo("item_1"));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    final GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    final Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map.size(), equalTo(1));
    assertThat("Incorrect serialization.", map.get(new Utf8("key1")), equalTo(100.0));

  }


  @Test
  public void shouldSerializeRowWithNullCorrectly() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final Serializer<GenericRow> serializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false, schemaRegistryClient
        ).serializer();

    final List columns = Arrays.asList(
        1511897796092L, 1L, null, 10.0, Arrays.asList(100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    final byte[] serializedRow = serializer.serialize("t1", genericRow);
    final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    final GenericRecord genericRecord =
        (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()), equalTo
        (null));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    final GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    final Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map,
               equalTo(Collections.singletonMap(new Utf8("key1"), 100.0)));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeRowWithNullValues() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final Serializer<GenericRow> serializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false, schemaRegistryClient
        ).serializer();

    final List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null, null);

    final GenericRow genericRow = new GenericRow(columns);
    serializer.serialize("t1", genericRow);

  }

  @Test
  public void shouldFailForIncompatibleType() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final Serializer<GenericRow> serializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false, schemaRegistryClient
        ).serializer();

    final List columns = Arrays.asList(
        1511897796092L, 1L, "item_1", "10.0", Arrays.asList((Double)100.0),
        Collections.singletonMap("key1", 100.0));

    final GenericRow genericRow = new GenericRow(columns);
    try {
      serializer.serialize("t1", genericRow);
      Assert.fail("Did not fail for incompatible types.");
    } catch (SerializationException e) {
    }

  }
}
