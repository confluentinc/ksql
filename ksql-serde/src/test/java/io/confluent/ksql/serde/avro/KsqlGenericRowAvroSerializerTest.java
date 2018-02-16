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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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


  org.apache.kafka.connect.data.Schema schema;

  @Before
  public void before() {

    schema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .field("arraycol".toUpperCase(), SchemaBuilder.array(org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .field("mapcol".toUpperCase(), SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .build();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KsqlGenericRowAvroSerializer ksqlGenericRowAvroSerializer = new KsqlGenericRowAvroSerializer
        (schema, schemaRegistryClient, new KsqlConfig(new HashMap<>()));

    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);
    byte[] serializedRow = ksqlGenericRowAvroSerializer.serialize("t1", genericRow);
    KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    GenericRecord genericRecord = (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()).toString(), equalTo("item_1"));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map.size(), equalTo(1));
    assertThat("Incorrect serialization.", map.get(new Utf8("key1")), equalTo(100.0));

  }


  @Test
  public void shouldSerializeRowWithNullCorrectly() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KsqlGenericRowAvroSerializer ksqlGenericRowAvroSerializer = new KsqlGenericRowAvroSerializer
        (schema, schemaRegistryClient, new KsqlConfig(new HashMap<>()));

    List columns = Arrays.asList(1511897796092L, 1L, null, 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);
    byte[] serializedRow = ksqlGenericRowAvroSerializer.serialize("t1", genericRow);
    KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    GenericRecord genericRecord = (GenericRecord) kafkaAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(genericRecord);
    assertThat("Incorrect serialization.", genericRecord.get("ordertime".toUpperCase()), equalTo
        (1511897796092L));
    assertThat("Incorrect serialization.", genericRecord.get("orderid".toUpperCase()), equalTo
        (1L));
    assertThat("Incorrect serialization.", genericRecord.get("itemid".toUpperCase()), equalTo
        (null));
    assertThat("Incorrect serialization.", genericRecord.get("orderunits".toUpperCase()), equalTo
        (10.0));

    GenericData.Array array = (GenericData.Array) genericRecord.get("arraycol".toUpperCase());
    Map map = (Map) genericRecord.get("mapcol".toUpperCase());

    assertThat("Incorrect serialization.", array.size(), equalTo(1));
    assertThat("Incorrect serialization.", array.get(0), equalTo(100.0));
    assertThat("Incorrect serialization.", map,
               equalTo(Collections.singletonMap(new Utf8("key1"), 100.0)));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSerializeRowWithNullValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KsqlGenericRowAvroSerializer ksqlGenericRowAvroSerializer = new KsqlGenericRowAvroSerializer
        (schema, schemaRegistryClient, new KsqlConfig(new HashMap<>()));

    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null, null);

    GenericRow genericRow = new GenericRow(columns);
    ksqlGenericRowAvroSerializer.serialize("t1", genericRow);

  }

  @Test
  public void shouldFailForIncompatibleType() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    KsqlGenericRowAvroSerializer ksqlGenericRowAvroSerializer = new KsqlGenericRowAvroSerializer
        (schema, schemaRegistryClient, new KsqlConfig(new HashMap<>()));

    List columns = Arrays.asList(1511897796092L, 1L, "item_1", "10.0", new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);
    try {
      byte[] serilizedRow = ksqlGenericRowAvroSerializer.serialize("t1", genericRow);
      Assert.fail("Did not fail for incompatible types.");
    } catch (Exception e) {
      assertThat(e.getMessage(), equalTo("org.apache.kafka.common.errors.SerializationException: Error serializing Avro message"));
    }

  }

}
