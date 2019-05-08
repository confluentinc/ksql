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

package io.confluent.ksql.serde.json;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.SerdeTestUtils;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlJsonDeserializerTest {

  private static final Schema orderSchema = SchemaBuilder.struct()
      .field("ordertime".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
      .field("orderid".toUpperCase(), Schema.OPTIONAL_INT64_SCHEMA)
      .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
      .field("orderunits".toUpperCase(), Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("arraycol".toUpperCase(), SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field("mapcol".toUpperCase(), SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .build();

  private static final Map<String, Object> AN_ORDER = ImmutableMap.<String, Object>builder()
      .put("ordertime", 1511897796092L)
      .put("@orderid", 1L)
      .put("itemid", "Item_1")
      .put("orderunits", 10.0)
      .put("arraycol", ImmutableList.of(10.0, 20.0))
      .put("mapcol", Collections.singletonMap("key1", 10.0))
      .build();

  private KsqlJsonDeserializer deserializer;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ProcessingLogConfig processingLogConfig
      = new ProcessingLogConfig(Collections.emptyMap());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private ProcessingLogger recordLogger;

  @Before
  public void before() {
    deserializer = new KsqlJsonDeserializer(orderSchema, recordLogger);
  }

  @Test
  public void shouldDeserializeJsonCorrectly() throws JsonProcessingException {
    // Given:
    final byte[] jsonBytes = objectMapper.writeValueAsBytes(AN_ORDER);

    // When:
    final Struct result = deserializer.deserialize("", jsonBytes);

    // Then:
    assertThat(result.schema(), is(orderSchema));
    assertThat(result.get(orderSchema.fields().get(0)), is(1511897796092L));
    assertThat(result.get(orderSchema.fields().get(1)), is(1L));
    assertThat(result.get(orderSchema.fields().get(2)), is("Item_1"));
    assertThat(result.get(orderSchema.fields().get(3)), is(10.0));
    assertThat(result.get(orderSchema.fields().get(4)), is(ImmutableList.of(10.0, 20.0)));
    assertThat(result.get(orderSchema.fields().get(5)), is(ImmutableMap.of("key1", 10.0)));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.put("extraField", "should be ignored");

    final byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    // When:
    final Struct result = deserializer.deserialize("", jsonBytes);

    // Then:
    assertThat(result.get(orderSchema.fields().get(0)), is(1511897796092L));
    assertThat(result.get(orderSchema.fields().get(1)), is(1L));
    assertThat(result.get(orderSchema.fields().get(2)), is("Item_1"));
    assertThat(result.get(orderSchema.fields().get(3)), is(10.0));
    assertThat(result.get(orderSchema.fields().get(4)), is(ImmutableList.of(10.0, 20.0)));
    assertThat(result.get(orderSchema.fields().get(5)), is(ImmutableMap.of("key1", 10.0)));
  }

  @Test
  public void shouldDeserializeEvenWithMissingFields() throws JsonProcessingException {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.remove("ordertime");

    final byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    // When:
    final Struct result = deserializer.deserialize("", jsonBytes);
    
    // Then:
    assertThat(result.get(orderSchema.fields().get(0)), is(nullValue()));
    assertThat(result.get(orderSchema.fields().get(1)), is(1L));
    assertThat(result.get(orderSchema.fields().get(2)), is("Item_1"));
    assertThat(result.get(orderSchema.fields().get(3)), is(10.0));
    assertThat(result.get(orderSchema.fields().get(4)), is(ImmutableList.of(10.0, 20.0)));
    assertThat(result.get(orderSchema.fields().get(5)), is(ImmutableMap.of("key1", 10.0)));
  }

  @Test
  public void shouldTreatNullAsNull() throws JsonProcessingException {
    // Given:
    final Map<String, Object> row = new HashMap<>();
    row.put("ordertime", null);
    row.put("@orderid", null);
    row.put("itemid", null);
    row.put("orderunits", null);
    row.put("arrayCol", new Double[]{0.0, null});
    row.put("mapCol", null);

    final byte[] bytes = objectMapper.writeValueAsBytes(row);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get(orderSchema.fields().get(0)), is(nullValue()));
    assertThat(result.get(orderSchema.fields().get(1)), is(nullValue()));
    assertThat(result.get(orderSchema.fields().get(2)), is(nullValue()));
    assertThat(result.get(orderSchema.fields().get(3)), is(nullValue()));
    assertThat(result.get(orderSchema.fields().get(4)), is(Arrays.asList(0.0, null)));
    assertThat(result.get(orderSchema.fields().get(5)), is(nullValue()));
  }

  @Test
  public void shouldCreateJsonStringForStructIfDefinedAsVarchar() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("itemid".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(schema, recordLogger);

    final byte[] bytes = "{\"itemid\":{\"CATEGORY\":{\"ID\":2,\"NAME\":\"Food\"},\"ITEMID\":6,\"NAME\":\"Item_6\"}}"
        .getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.schema(), is(schema));
    assertThat(result.get("ITEMID"),
        is("{\"CATEGORY\":{\"ID\":2,\"NAME\":\"Food\"},\"ITEMID\":6,\"NAME\":\"Item_6\"}"));
  }

  @Test
  public void shouldThrowIfTopLevelNotStruct() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("KSQL expects all top level schemas to be STRUCTs");

    // When:
    new KsqlJsonDeserializer(Schema.OPTIONAL_INT64_SCHEMA, recordLogger);
  }

  @Test
  public void shouldDeserializedTopLevelPrimitiveTypeIfSchemaHasOnlySingleField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(schema, recordLogger);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("id"), is(10));
  }

  @Test
  public void shouldThrowOnDeserializedTopLevelPrimitiveWhenSchemaHasMoreThanOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(schema, recordLogger);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(instanceOf(KsqlException.class));
    expectedException.expectCause(hasMessage(is("Expected JSON object not JSON value or array")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldDeserializeTopLevelArrayIfSchemaHasOnlySingleField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .build();

    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(schema, recordLogger);

    final byte[] bytes = "[1,2,3]".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("ids"), is(ImmutableList.of(1, 2, 3)));
  }

  @Test
  public void shouldThrowOnDeserializedTopLevelArrayWhenSchemaHasMoreThanOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final KsqlJsonDeserializer deserializer = new KsqlJsonDeserializer(schema, recordLogger);

    final byte[] bytes = "[1,2,3]".getBytes(StandardCharsets.UTF_8);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(instanceOf(KsqlException.class));
    expectedException.expectCause(hasMessage(is("Expected JSON object not JSON value or array")));

    // When:
    deserializer.deserialize("", bytes);
  }

  @Test
  public void shouldLogDeserializationErrors() {
    // Given:
    final byte[] data = "{foo".getBytes(StandardCharsets.UTF_8);
    try {
      // When:
      deserializer.deserialize("", data);
      fail("deserialize should have thrown");
    } catch (final SerializationException e) {
      // Then:
      SerdeTestUtils.shouldLogError(
          recordLogger,
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(
              e.getCause(),
              Optional.ofNullable(data)).apply(processingLogConfig),
          processingLogConfig);
    }
  }
}