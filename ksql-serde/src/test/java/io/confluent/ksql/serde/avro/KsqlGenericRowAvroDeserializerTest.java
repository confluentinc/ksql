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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.easymock.EasyMock;
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
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;


public class KsqlGenericRowAvroDeserializerTest {

  String schemaStr = "{"
                     + "\"namespace\": \"kql\","
                     + " \"name\": \"orders\","
                     + " \"type\": \"record\","
                     + " \"fields\": ["
                     + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                     + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                     + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                     + "     {\"name\": \"orderUnits\", \"type\": \"double\"},"
                     + "     {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": "
                     + "\"double\"}},"
                     + "     {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": "
                     + "\"double\"}}"
                     + " ]"
                     + "}";


  GenericRecord genericRecord = null;
  org.apache.kafka.connect.data.Schema schema;
  Schema avroSchema;

  @Before
  public void before() {
    Schema.Parser parser = new Schema.Parser();
    avroSchema = parser.parse(schemaStr);
    genericRecord  = new GenericData.Record(avroSchema);
    genericRecord.put("orderTime", 1511897796092L);
    genericRecord.put("orderId", 1L);
    genericRecord.put("itemId", "item_1");
    genericRecord.put("orderUnits", 10.0);
    genericRecord.put("arrayCol", new GenericData.Array(Schema.createArray(
        Schema.create(Schema.Type.DOUBLE)),
                                                        Collections.singletonList(100.0)));
    genericRecord.put("mapCol", Collections.singletonMap("key1", 100.0));

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
  public void shouldCreateCorrectRow() {

    KafkaAvroDeserializer kafkaAvroDeserializer = EasyMock.mock(KafkaAvroDeserializer.class);
    EasyMock.expect(kafkaAvroDeserializer.deserialize(EasyMock.anyString(), EasyMock.anyObject())
    ).andReturn(genericRecord);
    expectLastCall();
    replay(kafkaAvroDeserializer);

    KsqlGenericRowAvroDeserializer ksqlGenericRowAvroDeserializer = new
        KsqlGenericRowAvroDeserializer(schema, kafkaAvroDeserializer, false);

    GenericRow genericRow = ksqlGenericRowAvroDeserializer.deserialize("", new byte[]{});

    assertThat("Column number does not match.", genericRow.getColumns().size(), equalTo(6));
    assertThat("Invalid column value.", genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Invalid column value.", genericRow.getColumns().get(1), equalTo(1L));
    assertThat("Invalid column value.", ((Double[])genericRow.getColumns().get(4))[0], equalTo
        (100.0));
    assertThat("Invalid column value.", ((Map<String, Double>)genericRow.getColumns().get(5))
                   .get("key1"),
               equalTo
        (100.0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldDeserializeCorrectly() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);

    KsqlGenericRowAvroDeserializer ksqlGenericRowAvroDeserializer = new
        KsqlGenericRowAvroDeserializer(schema, schemaRegistryClient, false);

    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema, genericRow);

    GenericRow row = ksqlGenericRowAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", row.getColumns().get(2), equalTo
        ( "item_1"));
    assertThat("Incorrect deserializarion", row.getColumns().get(3), equalTo
        ( 10.0));
    assertThat("Incorrect deserializarion", ((Double[])row.getColumns().get(4)).length, equalTo
        (1));
    assertThat("Incorrect deserializarion", ((Map)row.getColumns().get(5)).size(), equalTo
        (1));


  }


  @Test
  public void shouldDeserializeIfThereAreRedundantFields() {
    org.apache.kafka.connect.data.Schema newSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .build();
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);

    KsqlGenericRowAvroDeserializer ksqlGenericRowAvroDeserializer = new
        KsqlGenericRowAvroDeserializer(newSchema, schemaRegistryClient, false);

    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema, genericRow);
    GenericRow row = ksqlGenericRowAvroDeserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(4));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", (String)row.getColumns().get(2), equalTo
        ( "item_1"));
  }


  @Test
  public void shouldDeserializeWithMissingFields() {
    String schemaStr1 = "{"
                        + "\"namespace\": \"kql\","
                        + " \"name\": \"orders\","
                        + " \"type\": \"record\","
                        + " \"fields\": ["
                        + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                        + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                        + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                        + "     {\"name\": \"orderUnits\", \"type\": \"double\"}"
                        + " ]"
                        + "}";
    Schema.Parser parser = new Schema.Parser();
    Schema avroSchema1 = parser.parse(schemaStr1);
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0);

    GenericRow genericRow = new GenericRow(columns);
    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema1, genericRow);

    KsqlGenericRowAvroDeserializer ksqlGenericRowAvroDeserializer = new
        KsqlGenericRowAvroDeserializer(schema, schemaRegistryClient, false);

    GenericRow row = ksqlGenericRowAvroDeserializer.deserialize("t1", serializedRow);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", (String)row.getColumns().get(2), equalTo
        ( "item_1"));
    Assert.assertNull(row.getColumns().get(4));
    Assert.assertNull(row.getColumns().get(5));
  }

  private byte[] getSerializedRow(String topicName, SchemaRegistryClient schemaRegistryClient,
                                  Schema rowAvroSchema,
                                  GenericRow
      genericRow) {
    Map map = new HashMap();
    // Automatically register the schema in the Schema Registry if it has not been registered.
    map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient, map);
    GenericRecord avroRecord = new GenericData.Record(rowAvroSchema);
    List<Schema.Field> fields = rowAvroSchema.getFields();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      if (fields.get(i).schema().getType() == Schema.Type.ARRAY) {
        avroRecord.put(fields.get(i).name(), Arrays.asList((Object[]) genericRow.getColumns().get(i)));
      } else {
        avroRecord.put(fields.get(i).name(), genericRow.getColumns().get(i));
      }
    }

    return kafkaAvroSerializer.serialize(topicName, avroRecord);
  }

}
