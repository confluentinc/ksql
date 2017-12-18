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
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.ksql.GenericRow;

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

  @Before
  public void before() {
    Schema.Parser parser = new Schema.Parser();
    Schema avroSchema = parser.parse(schemaStr);
    genericRecord  = new GenericData.Record(avroSchema);
    genericRecord.put("orderTime", 1511897796092l);
    genericRecord.put("orderId", 1l);
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
    assertThat("Invalid column value.", genericRow.getColumns().get(0), equalTo(1511897796092l));
    assertThat("Invalid column value.", genericRow.getColumns().get(1), equalTo(1l));
    assertThat("Invalid column value.", ((Double[])genericRow.getColumns().get(4))[0], equalTo
        (100.0));
    assertThat("Invalid column value.", ((Map<String, Double>)genericRow.getColumns().get(5))
                   .get("key1"),
               equalTo
        (100.0));
  }

}
