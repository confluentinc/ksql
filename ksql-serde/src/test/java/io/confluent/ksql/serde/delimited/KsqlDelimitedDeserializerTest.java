/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KsqlDelimitedDeserializerTest {

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
  public void shouldDeserializeDelimitedCorrectly() {
    final String rowString = "1511897796092,1,item_1,10.0\r\n";

    final KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    final GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat((String) genericRow.getColumns().get(2), equalTo("item_1"));
    assertThat((Double) genericRow.getColumns().get(3), equalTo(10.0));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {

    final String rowString = "1511897796092,1,item_1,\r\n";

    final KsqlDelimitedDeserializer ksqlJsonDeserializer = new KsqlDelimitedDeserializer(orderSchema);

    final GenericRow genericRow = ksqlJsonDeserializer.deserialize("", rowString.getBytes());
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat((Long) genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat((Long) genericRow.getColumns().get(1), equalTo(1L));
    assertThat((String) genericRow.getColumns().get(2), equalTo("item_1"));
    Assert.assertNull(genericRow.getColumns().get(3));
  }

}
