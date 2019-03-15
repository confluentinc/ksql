/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.SerdeTestUtils;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KsqlDelimitedDeserializerTest {
  private Schema orderSchema;
  private final ProcessingLogConfig processingLogConfig
      = new ProcessingLogConfig(Collections.emptyMap());
  private KsqlDelimitedDeserializer delimitedDeserializer;

  @Mock
  private ProcessingLogger recordLogger;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void before() {
    orderSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();
    delimitedDeserializer = new KsqlDelimitedDeserializer(
        orderSchema,
        recordLogger);
  }

  @Test
  public void shouldDeserializeDelimitedCorrectly() {
    final String rowString = "1511897796092,1,item_1,10.0\r\n";

    final GenericRow genericRow = delimitedDeserializer.deserialize(
        "",
        rowString.getBytes(StandardCharsets.UTF_8));
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat(genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat(genericRow.getColumns().get(1), equalTo(1L));
    assertThat(genericRow.getColumns().get(2), equalTo("item_1"));
    assertThat(genericRow.getColumns().get(3), equalTo(10.0));
  }

  @Test
  public void shouldLogErrors() {
    Throwable cause = null;
    final byte[] record = "badnumfields".getBytes(StandardCharsets.UTF_8);
    try {
      delimitedDeserializer.deserialize("topic", record);
      fail("deserialize should have thrown");
    } catch (final SerializationException e) {
      cause = e.getCause();
    }
    SerdeTestUtils.shouldLogError(
        recordLogger,
        SerdeProcessingLogMessageFactory.deserializationErrorMsg(
            cause,
            Optional.ofNullable(record)).apply(processingLogConfig),
        processingLogConfig);
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() {
    final String rowString = "1511897796092,1,item_1,\r\n";

    final GenericRow genericRow = delimitedDeserializer.deserialize(
        "",
        rowString.getBytes(StandardCharsets.UTF_8));
    assertThat(genericRow.getColumns().size(), equalTo(4));
    assertThat(genericRow.getColumns().get(0), equalTo(1511897796092L));
    assertThat(genericRow.getColumns().get(1), equalTo(1L));
    assertThat(genericRow.getColumns().get(2), equalTo("item_1"));
    Assert.assertNull(genericRow.getColumns().get(3));
  }

}
