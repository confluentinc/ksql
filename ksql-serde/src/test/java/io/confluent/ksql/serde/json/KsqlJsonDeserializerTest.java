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

package io.confluent.ksql.serde.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.GenericRow;

public class KsqlJsonDeserializerTest {

  Schema orderSchema;

  @Before
  public void before() {

    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .field("arraycol".toUpperCase(), SchemaBuilder.array(org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .field("mapcol".toUpperCase(), SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .build();
  }

  @Test
  public void shouldDeserializeJsonCorrectly() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> orderRow = new HashMap<>();
    orderRow.put("ordertime", 1511897796092L);
    orderRow.put("@orderid", 1L);
    orderRow.put("itemid", "Item_1");
    orderRow.put("orderunits", 10.0);
    orderRow.put("arraycol", new Double[]{10.0, 20.0});
    orderRow.put("mapcol", Collections.singletonMap("key1", 10.0));

    byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    KsqlJsonDeserializer ksqlJsonDeserializer = new KsqlJsonDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", jsonBytes);
    assertThat(genericRow.getColumns().size(), equalTo(6));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat(((String) genericRow.getColumns().get(2)), equalTo("Item_1"));
    assertThat((Double) genericRow.getColumns().get(3), equalTo(10.0));

  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> orderRow = new HashMap<>();
    orderRow.put("ordertime", 1511897796092L);
    orderRow.put("@orderid", 1L);
    orderRow.put("itemid", "Item_1");
    orderRow.put("orderunits", 10.0);
    orderRow.put("arraycol", new Double[]{10.0, 20.0});
    orderRow.put("mapcol", Collections.singletonMap("key1", 10.0));

    byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    Schema newOrderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .build();
    KsqlJsonDeserializer ksqlJsonDeserializer = new KsqlJsonDeserializer(newOrderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", jsonBytes);
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat(((String) genericRow.getColumns().get(2)), equalTo("Item_1"));
    assertThat((Double) genericRow.getColumns().get(3), equalTo(10.0));

  }

  @Test
  public void shouldDeserializeEvenWithMissingFields() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> orderRow = new HashMap<>();
    orderRow.put("ordertime", 1511897796092L);
    orderRow.put("@orderid", 1L);
    orderRow.put("itemid", "Item_1");
    orderRow.put("orderunits", 10.0);

    byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    KsqlJsonDeserializer ksqlJsonDeserializer = new KsqlJsonDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", jsonBytes);
    assertThat(genericRow.getColumns().size(), equalTo(6));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat((String) genericRow.getColumns().get(2), equalTo("Item_1"));
    assertThat((Double) genericRow.getColumns().get(3), equalTo(10.0));
    Assert.assertNull(genericRow.getColumns().get(4));
    Assert.assertNull(genericRow.getColumns().get(5));
  }

  @Test
  public void shouldTreatNullAsNull() throws JsonProcessingException {
    final ObjectMapper objectMapper = new ObjectMapper();
    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(orderSchema);
    Map<String, Object> row = new HashMap<>();
    row.put("ordertime", null);
    row.put("@orderid", null);
    row.put("itemid", null);
    row.put("orderunits", null);
    row.put("arrayCol", new Double[]{0.0, null});
    row.put("mapCol", null);

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, null, null, new Double[]{0.0, null}, null));
    GenericRow genericRow = deserializer.deserialize("", objectMapper.writeValueAsBytes(row));
    assertThat(genericRow, equalTo(expected));

  }
}
