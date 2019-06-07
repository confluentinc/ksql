/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.rest.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class StructSerializationModuleTest {

  private final Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  private final Schema categorySchema = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .optional().build();

  private final Schema itemInfoSchema = SchemaBuilder.struct()
      .field("ITEMID", Schema.INT64_SCHEMA)
      .field("NAME", Schema.STRING_SCHEMA)
      .field("CATEGORY", categorySchema)
      .optional().build();

  private ObjectMapper objectMapper;
  @Before
  public void init() {
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new StructSerializationModule());
  }

  @Test
  public void shouldSerializeStructCorrectly() throws JsonProcessingException {

    final Struct address = new Struct(addressSchema);
    address.put("NUMBER", 101L);
    address.put("STREET", "University Ave.");
    address.put("CITY", "Palo Alto");
    address.put("STATE", "CA");
    address.put("ZIPCODE", 94301L);

    final byte[] serializedBytes = objectMapper.writeValueAsBytes(address);
    final String jsonString = new String(serializedBytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo("{\"NUMBER\":101,\"STREET\":\"University Ave.\",\"CITY\":\"Palo Alto\",\"STATE\":\"CA\",\"ZIPCODE\":94301}"));
  }

  @Test
  public void shouldSerializeStructWithNestedStructCorrectly() throws JsonProcessingException {
    final Struct category = new Struct(categorySchema);
    category.put("ID", 1L);
    category.put("NAME", "Food");

    final Struct item = new Struct(itemInfoSchema);
    item.put("ITEMID", 1L);
    item.put("NAME", "ICE CREAM");
    item.put("CATEGORY", category);
    final byte[] serializedBytes = objectMapper.writeValueAsBytes(item);
    final String jsonString = new String(serializedBytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo("{\"ITEMID\":1,\"NAME\":\"ICE CREAM\",\"CATEGORY\":{\"ID\":1,\"NAME\":\"Food\"}}"));
  }

  @Test
  public void shouldSerializeStructWithNestedStructAndNullFieldsCorrectly() throws JsonProcessingException {
    final Struct category = new Struct(categorySchema);
    category.put("ID", 1L);
    category.put("NAME", "Food");

    final Struct item = new Struct(itemInfoSchema);
    item.put("ITEMID", 1L);
    item.put("NAME", "ICE CREAM");
    item.put("CATEGORY", null);
    final byte[] serializedBytes = objectMapper.writeValueAsBytes(item);
    final String jsonString = new String(serializedBytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo("{\"ITEMID\":1,\"NAME\":\"ICE CREAM\",\"CATEGORY\":null}"));
  }

  @Test
  public void shouldSerializeStructInsideListCorrectly() throws JsonProcessingException {
    final Struct category = new Struct(categorySchema);
    category.put("ID", 1L);
    category.put("NAME", "Food");

    final Struct item = new Struct(itemInfoSchema);
    item.put("ITEMID", 1L);
    item.put("NAME", "ICE CREAM");
    item.put("CATEGORY", null);

    final List<Object> list = new ArrayList<>();
    list.add("Hello");
    list.add(1);
    list.add(1L);
    list.add(1.0);
    list.add(item);

    final byte[] serializedBytes = objectMapper.writeValueAsBytes(list);
    final String jsonString = new String(serializedBytes, StandardCharsets.UTF_8);
    assertThat(jsonString, equalTo("[\"Hello\",1,1,1.0,{\"ITEMID\":1,\"NAME\":\"ICE CREAM\",\"CATEGORY\":null}]"));
  }
}