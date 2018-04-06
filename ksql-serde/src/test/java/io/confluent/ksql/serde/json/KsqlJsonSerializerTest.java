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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.GenericRow;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class KsqlJsonSerializerTest {

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
  public void shouldSerializeRowCorrectly() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, Arrays.asList(100.0),
                                 Collections.singletonMap("key1", 100.0));
    GenericRow genericRow = new GenericRow(columns);
    KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo("{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":[100.0],\"MAPCOL\":{\"key1\":100.0}}"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, null,
                                 null);
    GenericRow genericRow = new GenericRow(columns);
    KsqlJsonSerializer ksqlJsonDeserializer = new KsqlJsonSerializer(orderSchema);
    byte[] bytes = ksqlJsonDeserializer.serialize("t1", genericRow);

    String jsonString = new String(bytes);
    assertThat("Incorrect serialization.", jsonString, equalTo("{\"ORDERTIME\":1511897796092,\"ORDERID\":1,\"ITEMID\":\"item_1\",\"ORDERUNITS\":10.0,\"ARRAYCOL\":null,\"MAPCOL\":null}"));
  }

}
