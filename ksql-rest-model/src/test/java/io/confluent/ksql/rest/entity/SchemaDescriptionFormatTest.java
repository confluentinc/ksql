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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class SchemaDescriptionFormatTest {

  private void shouldSerializeCorrectly(final String descriptionString,
                                        final List<FieldInfo> deserialized) throws IOException {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final List<?> deserializedGeneric = objectMapper.readValue(descriptionString, List.class);
    final String serialized=  objectMapper.writeValueAsString(deserialized);
    assertThat(
        objectMapper.readValue(serialized, List.class),
        equalTo(deserializedGeneric));
  }

  @Test
  public void shouldFormatSchemaEntityCorrectlyForStruct() throws IOException {
    final String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"l1integer\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"INTEGER\",\n" +
        "      \"fields\": null,\n" +
        "      \"memberSchema\": null\n" +
        "    }\n" +
        "  },\n" +
        "  {\n" +
        "    \"name\": \"l1struct\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"STRUCT\",\n" +
        "      \"fields\": [\n" +
        "        {\n" +
        "          \"name\": \"l2string\",\n" +
        "          \"schema\": {\n" +
        "            \"type\": \"STRING\",\n" +
        "            \"fields\": null,\n" +
        "            \"memberSchema\": null\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"name\": \"l2integer\",\n" +
        "          \"schema\": {\n" +
        "            \"type\": \"INTEGER\",\n" +
        "            \"fields\": null,\n" +
        "            \"memberSchema\": null\n" +
        "          }\n" +
        "        }\n" +
        "      ],\n" +
        "      \"memberSchema\": null\n" +
        "    }\n" +
        "  }\n" +
        "]";

    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final List<FieldInfo> deserialized = objectMapper.readValue(
        descriptionString, new TypeReference<List<FieldInfo>>(){});

    // Test deserialization
    assertThat(deserialized.size(), equalTo(2));
    assertThat(deserialized.get(0).getName(), equalTo("l1integer"));
    assertThat(
        deserialized.get(0).getSchema(),
        equalTo(
            new SchemaInfo(SqlBaseType.INTEGER, null, null)));
    assertThat(deserialized.get(1).getName(), equalTo("l1struct"));
    final SchemaInfo structSchema = deserialized.get(1).getSchema();
    assertThat(structSchema.getType(), equalTo(SqlBaseType.STRUCT));
    assertThat(structSchema.getMemberSchema(), equalTo(Optional.empty()));
    assertThat(structSchema.getFields().get().size(), equalTo(2));
    assertThat(structSchema.getFields().get().get(0).getName(), equalTo("l2string"));
    assertThat(
        structSchema.getFields().get().get(0).getSchema(),
        equalTo(
            new SchemaInfo(SqlBaseType.STRING, null, null)));
    assertThat(structSchema.getFields().get().get(1).getName(), equalTo("l2integer"));
    assertThat(
        structSchema.getFields().get().get(1).getSchema(),
        equalTo(
            new SchemaInfo(SqlBaseType.INTEGER, null, null)));

    shouldSerializeCorrectly(descriptionString, deserialized);
  }

  @Test
  public void shouldFormatSchemaEntityCorrectlyForMap() throws IOException {
    final String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"mapfield\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"MAP\",\n" +
        "      \"memberSchema\": {\n" +
        "        \"type\": \"STRING\",\n" +
        "        \"memberSchema\": null,\n" +
        "        \"fields\": null\n" +
        "      },\n" +
        "      \"fields\": null\n" +
        "    }\n" +
        "  }\n" +
        "]";

    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final List<FieldInfo> deserialized = objectMapper.readValue(
        descriptionString, new TypeReference<List<FieldInfo>>(){});

    // Test deserialization
    assertThat(deserialized.size(), equalTo(1));
    assertThat(deserialized.get(0).getName(), equalTo("mapfield"));
    assertThat(
        deserialized.get(0).getSchema(),
        equalTo(
            new SchemaInfo(
                SqlBaseType.MAP,
                null,
                new SchemaInfo(SqlBaseType.STRING, null, null))));

    shouldSerializeCorrectly(descriptionString, deserialized);
  }

  @Test
  public void shouldFormatSchemaEntityCorrectlyForArray() throws IOException {
    final String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"arrayfield\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"ARRAY\",\n" +
        "      \"memberSchema\": {\n" +
        "        \"type\": \"STRING\",\n" +
        "        \"memberSchema\": null,\n" +
        "        \"fields\": null\n" +
        "      },\n" +
        "      \"fields\": null\n" +
        "    }\n" +
        "  }\n" +
        "]";

    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final List<FieldInfo> deserialized = objectMapper.readValue(
        descriptionString, new TypeReference<List<FieldInfo>>(){});

    // Test deserialization
    assertThat(deserialized.size(), equalTo(1));
    assertThat(deserialized.get(0).getName(), equalTo("arrayfield"));
    assertThat(
        deserialized.get(0).getSchema(),
        equalTo(
            new SchemaInfo(
                SqlBaseType.ARRAY,
                null,
                new SchemaInfo(SqlBaseType.STRING, null, null))));

    shouldSerializeCorrectly(descriptionString, deserialized);
  }
}
