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
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.SerdeTestUtils;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

  @Mock
  ProcessingLogger recordLogger;

  @Before
  public void before() {
    deserializer = new KsqlJsonDeserializer(orderSchema, recordLogger);
  }

  @Test
  public void shouldDeserializeJsonCorrectly() throws JsonProcessingException {
    // Given:
    final byte[] jsonBytes = objectMapper.writeValueAsBytes(AN_ORDER);

    // When:
    final GenericRow genericRow = deserializer.deserialize("", jsonBytes);

    // Then:
    assertThat(genericRow.getColumns(), hasSize(6));
    assertThat(genericRow.getColumns().get(0), is(1511897796092L));
    assertThat(genericRow.getColumns().get(1), is(1L));
    assertThat(genericRow.getColumns().get(2), is("Item_1"));
    assertThat(genericRow.getColumns().get(3), is(10.0));
    assertThat(genericRow.getColumns().get(4), is(ImmutableList.of(10.0, 20.0)));
    assertThat(genericRow.getColumns().get(5), is(ImmutableMap.of("key1", 10.0)));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithRedundantFields() throws JsonProcessingException {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.put("extraField", "should be ignored");

    final byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    // When:
    final GenericRow genericRow = deserializer.deserialize("", jsonBytes);

    // Then:
    assertThat(genericRow.getColumns(), hasSize(6));
    assertThat(genericRow.getColumns().get(0), is(1511897796092L));
    assertThat(genericRow.getColumns().get(1), is(1L));
    assertThat(genericRow.getColumns().get(2), is("Item_1"));
    assertThat(genericRow.getColumns().get(3), is(10.0));
    assertThat(genericRow.getColumns().get(4), is(ImmutableList.of(10.0, 20.0)));
    assertThat(genericRow.getColumns().get(5), is(ImmutableMap.of("key1", 10.0)));
  }

  @Test
  public void shouldDeserializeEvenWithMissingFields() throws JsonProcessingException {
    // Given:
    final Map<String, Object> orderRow = new HashMap<>(AN_ORDER);
    orderRow.remove("ordertime");

    final byte[] jsonBytes = objectMapper.writeValueAsBytes(orderRow);

    // When:
    final GenericRow genericRow = deserializer.deserialize("", jsonBytes);
    
    // Then:
    assertThat(genericRow.getColumns(), hasSize(6));
    assertThat(genericRow.getColumns().get(0), is(nullValue()));
    assertThat(genericRow.getColumns().get(1), is(1L));
    assertThat(genericRow.getColumns().get(2), is("Item_1"));
    assertThat(genericRow.getColumns().get(3), is(10.0));
    assertThat(genericRow.getColumns().get(4), is(ImmutableList.of(10.0, 20.0)));
    assertThat(genericRow.getColumns().get(5), is(ImmutableMap.of("key1", 10.0)));
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
    final GenericRow genericRow = deserializer.deserialize("", bytes);

    // Then:
    final GenericRow expected = new GenericRow(
        Arrays.asList(null, null, null, null, new Double[]{0.0, null}, null));

    assertThat(genericRow, is(expected));
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
    final GenericRow genericRow = deserializer.deserialize("", bytes);

    // Then:
    final GenericRow expected = new GenericRow(Collections.singletonList(
        "{\"CATEGORY\":{\"ID\":2,\"NAME\":\"Food\"},\"ITEMID\":6,\"NAME\":\"Item_6\"}"));
    assertThat(genericRow, is(expected));
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