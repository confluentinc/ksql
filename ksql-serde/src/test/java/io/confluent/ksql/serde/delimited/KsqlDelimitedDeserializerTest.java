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

package io.confluent.ksql.serde.delimited;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.GenericRow;

public class KsqlDelimitedDeserializerTest {

  Schema orderSchema;

  @Before
  public void before() {

    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .build();
  }

  @Test
  public void shouldDeserializeDelimitedCorrectly() {
    String rowString = "1511897796092,1,'item_1',10.0";

    KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    Assert.assertTrue(genericRow.getColumns().size() == 4);
    Assert.assertTrue((Long) genericRow.getColumns().get(0) == 1511897796092L);
    Assert.assertTrue((Long) genericRow.getColumns().get(1) == 1L);
    Assert.assertTrue(((String) genericRow.getColumns().get(2)).equals("item_1"));
    Assert.assertTrue((Double) genericRow.getColumns().get(3) == 10.0);

  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {

    String rowString = "1511897796092,1,'item_1',null";

    KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    Assert.assertTrue(genericRow.getColumns().size() == 4);
    Assert.assertTrue((Long) genericRow.getColumns().get(0) == 1511897796092L);
    Assert.assertTrue((Long) genericRow.getColumns().get(1) == 1L);
    Assert.assertTrue(((String) genericRow.getColumns().get(2)).equals("item_1"));
    Assert.assertNull(genericRow.getColumns().get(3));
  }

}
