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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlJsonSerializerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Schema ORDER_SCHEMA = SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("ARRAYCOL", SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field("MAPCOL", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .build();

  private static final Schema ADDRESS_SCHEMA = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  private static final Schema CATEGORY_SCHEMA = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .optional().build();

  private static final Schema ITEM_SCHEMA = SchemaBuilder.struct()
      .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CATEGORIES", SchemaBuilder.array(CATEGORY_SCHEMA).optional().build())
      .optional().build();

  private static final Schema SCHEMA_WITH_STRUCT = SchemaBuilder.struct()
      .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
      .field("orderid", Schema.OPTIONAL_INT64_SCHEMA)
      .field("itemid", ITEM_SCHEMA)
      .field("orderunits", Schema.OPTIONAL_INT32_SCHEMA)
      .field("arraycol", SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field("mapcol", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
      .field("address", ADDRESS_SCHEMA)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlJsonSerializer serializer;

  @Before
  public void before() {
    serializer = new KsqlJsonSerializer(ORDER_SCHEMA);
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    // Given:
    final Struct struct = new Struct(ORDER_SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0)
        .put("ARRAYCOL", Collections.singletonList(100.0))
        .put("MAPCOL", Collections.singletonMap("key1", 100.0));

    // When:
    final byte[] bytes = serializer.serialize("t1", struct);

    // Then:
    final String jsonString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo(
        "{"
            + "\"ORDERTIME\":1511897796092,"
            + "\"ORDERID\":1,"
            + "\"ITEMID\":\"item_1\","
            + "\"ORDERUNITS\":10.0,"
            + "\"ARRAYCOL\":[100.0],"
            + "\"MAPCOL\":{\"key1\":100.0}"
            + "}"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    // Given:
    final Struct struct = new Struct(ORDER_SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0)
        .put("ARRAYCOL", null)
        .put("MAPCOL", null);

    // When:
    final byte[] bytes = serializer.serialize("t1", struct);

    // Then:
    final String jsonString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo(
        "{"
            + "\"ORDERTIME\":1511897796092,"
            + "\"ORDERID\":1,"
            + "\"ITEMID\":\"item_1\","
            + "\"ORDERUNITS\":10.0,"
            + "\"ARRAYCOL\":null,"
            + "\"MAPCOL\":null"
            + "}"));
  }

  @Test
  public void shouldHandleStruct() throws IOException {
    // Given:
    final Struct struct = buildWithNestedStruct();
    serializer = new KsqlJsonSerializer(SCHEMA_WITH_STRUCT);

    // When:
    final byte[] bytes = serializer.serialize("", struct);

    // Then:
    final JsonNode jsonNode = OBJECT_MAPPER.readTree(bytes);
    assertThat(jsonNode.size(), equalTo(7));
    assertThat(jsonNode.get("ordertime").asLong(), is(1234567L));
    assertThat(jsonNode.get("itemid").get("NAME").asText(), is("Item_10"));
  }

  @Test
  public void shouldSerializedTopLevelPrimitiveIfValueHasOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    final KsqlJsonSerializer serializer = new KsqlJsonSerializer(Schema.OPTIONAL_INT64_SCHEMA);

    final Struct value = new Struct(schema)
        .put("id", 10L);

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("10"));
  }

  @Test
  public void shouldThrowOnSerializedTopLevelPrimitiveWhenSchemaHasMoreThanOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT32_SCHEMA)
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final Struct value = new Struct(schema)
        .put("id", 10);

    final KsqlJsonSerializer serializer = new KsqlJsonSerializer(Schema.OPTIONAL_INT64_SCHEMA);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectMessage("Expected to serialize JSON value or array not JSON object");

    // When:
    serializer.serialize("", value);
  }

  @Test
  public void shouldSerializeTopLevelArrayIfValueHasOnlySingleField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build())
        .build();

    final Struct value = new Struct(schema)
        .put("ids", ImmutableList.of(1L, 2L, 3L));

    final KsqlJsonSerializer serializer = new KsqlJsonSerializer(SchemaBuilder
        .array(Schema.OPTIONAL_INT64_SCHEMA)
        .build());

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("[1,2,3]"));
  }

  @Test
  public void shouldThrowOnSerializedTopLevelArrayWhenSchemaHasMoreThanOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("ids", SchemaBuilder
            .array(Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build())
        .field("id2", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final Struct value = new Struct(schema)
        .put("ids", ImmutableList.of(1, 2, 3));

    final KsqlJsonSerializer serializer = new KsqlJsonSerializer(SchemaBuilder
        .array(Schema.OPTIONAL_INT64_SCHEMA)
        .build());

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectMessage("Expected to serialize JSON value or array not JSON object");

    // When:
    serializer.serialize("", value);
  }

  private static Struct buildWithNestedStruct() {
    final Struct topLevel = new Struct(SCHEMA_WITH_STRUCT);

    topLevel.put("ordertime", 1234567L);
    topLevel.put("orderid", 10L);

    final Struct category = new Struct(CATEGORY_SCHEMA);
    category.put("ID", Math.random() > 0.5 ? 1L : 2L);
    category.put("NAME", Math.random() > 0.5 ? "Produce" : "Food");

    final Struct item = new Struct(ITEM_SCHEMA);
    item.put("ITEMID", 10L);
    item.put("NAME", "Item_10");
    item.put("CATEGORIES", Collections.singletonList(category));

    topLevel.put("itemid", item);
    topLevel.put("orderunits", 10);
    topLevel.put("arraycol", Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0));

    final Map<String, Double> map = new HashMap<>();
    map.put("key1", 10.0);
    map.put("key2", 20.0);
    map.put("key3", 30.0);
    topLevel.put("mapcol", map);

    final Struct address = new Struct(ADDRESS_SCHEMA);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    topLevel.put("address", address);

    return topLevel;
  }
}
