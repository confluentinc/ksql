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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlDelimitedDeserializerTest {

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field("ordertime".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
      .field("orderid".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
      .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
      .field("orderunits".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private final ProcessingLogConfig processingLogConfig =
      new ProcessingLogConfig(Collections.emptyMap());

  private KsqlDelimitedDeserializer deserializer;

  @Mock
  private ProcessingLogger recordLogger;

  @Before
  public void before() {
    deserializer = new KsqlDelimitedDeserializer(ORDER_SCHEMA, recordLogger);
  }

  @Test
  public void shouldDeserializeDelimitedCorrectly() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final GenericRow genericRow = deserializer.deserialize("", bytes);

    // Then:
    assertThat(genericRow.getColumns().size(), is(4));
    assertThat(genericRow.getColumns().get(0), is(1511897796092L));
    assertThat(genericRow.getColumns().get(1), is(1L));
    assertThat(genericRow.getColumns().get(2), is("item_1"));
    assertThat(genericRow.getColumns().get(3), is(10.0));
  }

  @Test
  public void shouldLogErrors() {
    // Given:
    final byte[] record = "badnumfields".getBytes(StandardCharsets.UTF_8);

    try {
      // When:
      deserializer.deserialize("topic", record);
      fail("deserialize should have thrown");
    } catch (final SerializationException e) {

      // Then:
      SerdeTestUtils.shouldLogError(
          recordLogger,
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(
              e.getCause(),
              Optional.ofNullable(record)).apply(processingLogConfig),
          processingLogConfig);
    }
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithEmptyFields() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final GenericRow genericRow = deserializer.deserialize("", bytes);

    // Then:
    assertThat(genericRow.getColumns().size(), is(4));
    assertThat(genericRow.getColumns().get(0), is(1511897796092L));
    assertThat(genericRow.getColumns().get(1), is(1L));
    assertThat(genericRow.getColumns().get(2), is("item_1"));
    assertThat(genericRow.getColumns().get(3), is(nullValue()));
  }

  @Test(expected = SerializationException.class)
  public void shouldThrowIfRowHasTooFewColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test(expected = SerializationException.class)
  public void shouldThrowIfRowHasTooMayColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,extra\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    deserializer.deserialize("", bytes);
  }
}
