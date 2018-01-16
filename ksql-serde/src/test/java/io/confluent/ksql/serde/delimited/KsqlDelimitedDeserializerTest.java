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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

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
    String rowString = "1511897796092,1,item_1,10.0\r\n";

    KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat((String) genericRow.getColumns().get(2), equalTo("item_1"));
    assertThat((Double) genericRow.getColumns().get(3), equalTo(10.0));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {

    String rowString = "1511897796092,1,item_1,\r\n";

    KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat((String) genericRow.getColumns().get(2), equalTo("item_1"));
    Assert.assertNull(genericRow.getColumns().get(3));
  }

}
