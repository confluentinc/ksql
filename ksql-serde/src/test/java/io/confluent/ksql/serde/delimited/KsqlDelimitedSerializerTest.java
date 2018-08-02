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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class KsqlDelimitedSerializerTest {

  Schema orderSchema;

  @Before
  public void before() {

    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0);
    GenericRow genericRow = new GenericRow(columns);
    KsqlDelimitedSerializer ksqlDelimitedSerializer = new KsqlDelimitedSerializer(orderSchema);
    byte[] bytes = ksqlDelimitedSerializer.serialize("t1", genericRow);

    String delimitedString = new String(bytes);
    assertThat("Incorrect serialization.", delimitedString, equalTo("1511897796092,1,item_1,10.0"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", null);
    GenericRow genericRow = new GenericRow(columns);
    KsqlDelimitedSerializer ksqlDelimitedSerializer = new KsqlDelimitedSerializer(orderSchema);
    byte[] bytes = ksqlDelimitedSerializer.serialize("t1", genericRow);

    String delimitedString = new String(bytes);
    assertThat("Incorrect serialization.", delimitedString, equalTo("1511897796092,1,item_1,"));
  }
}
