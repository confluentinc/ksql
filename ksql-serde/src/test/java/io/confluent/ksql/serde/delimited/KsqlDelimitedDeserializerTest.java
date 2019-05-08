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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.SerdeTestUtils;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlDelimitedDeserializerTest {

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build();

  private final ProcessingLogConfig processingLogConfig =
      new ProcessingLogConfig(Collections.emptyMap());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(10.0));
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
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(nullValue()));
  }

  @Test
  public void shouldThrowIfRowHasTooFewColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1\r\n".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException
        .expectCause(hasMessage(is("Unexpected field count, csvFields:3 schemaFields:4")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowIfRowHasTooMayColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,extra\r\n".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException
        .expectCause(hasMessage(is("Unexpected field count, csvFields:5 schemaFields:4")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowIfTopLevelNotStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("KSQL expects all top level schemas to be STRUCTs");

    // When:
    new KsqlDelimitedDeserializer(Schema.OPTIONAL_INT64_SCHEMA, recordLogger);
  }

  @Test
  public void shouldDeserializedTopLevelPrimitiveTypeIfSchemaHasOnlySingleField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema,
        recordLogger);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("id"), CoreMatchers.is(10));
  }

  @Test
  public void shouldThrowOnDeserializedTopLevelPrimitiveWhenSchemaHasMoreThanOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final KsqlDelimitedDeserializer deserializer = new KsqlDelimitedDeserializer(schema,
        recordLogger);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(instanceOf(KsqlException.class));
    expectedException.expectCause(
        hasMessage(CoreMatchers.is("Unexpected field count, csvFields:1 schemaFields:2")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldThrowOnArrayTypes() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build();

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support complex type: ARRAY");

    // When:
    new KsqlDelimitedDeserializer(schema, recordLogger);
  }

  @Test
  public void shouldThrowOnMapTypes() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build();

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support complex type: MAP");

    // When:
    new KsqlDelimitedDeserializer(schema, recordLogger);
  }

  @Test
  public void shouldThrowOnStructTypes() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .struct()
            .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build();

    // Then:
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("DELIMITED does not support complex type: STRUCT");

    // When:
    new KsqlDelimitedDeserializer(schema, recordLogger);
  }
}
