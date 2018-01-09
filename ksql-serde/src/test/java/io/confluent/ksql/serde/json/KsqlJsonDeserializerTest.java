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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.GenericRow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

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
    assertThat("Incorrect columns count.", genericRow.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserialization", (Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserialization", (Long) genericRow.getColumns().get(1), equalTo( 1L));
    assertThat("Incorrect deserialization", ((String) genericRow.getColumns().get(2)), equalTo("Item_1"));
    assertThat("Incorrect deserialization", (Double) genericRow.getColumns().get(3), equalTo(10.0));

  }

}
