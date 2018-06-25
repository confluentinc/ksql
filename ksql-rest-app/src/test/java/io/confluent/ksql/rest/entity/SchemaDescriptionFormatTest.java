package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.util.JsonMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SchemaDescriptionFormatTest {
  private ObjectMapper newObjectMapper() {
    ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.registerModule(new Jdk8Module());
    return objectMapper;
  }

  private void shouldSerializeCorrectly(final String descriptionString,
                                        final List<FieldInfo> deserialized) throws IOException {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    final List deserializedGeneric = objectMapper.readValue(descriptionString, List.class);
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
            new SchemaInfo(SchemaInfo.Type.INTEGER, null, null)));
    assertThat(deserialized.get(1).getName(), equalTo("l1struct"));
    final SchemaInfo structSchema = deserialized.get(1).getSchema();
    assertThat(structSchema.getType(), equalTo(SchemaInfo.Type.STRUCT));
    assertThat(structSchema.getMemberSchema(), equalTo(Optional.empty()));
    assertThat(structSchema.getFields().get().size(), equalTo(2));
    assertThat(structSchema.getFields().get().get(0).getName(), equalTo("l2string"));
    assertThat(
        structSchema.getFields().get().get(0).getSchema(),
        equalTo(
            new SchemaInfo(SchemaInfo.Type.STRING, null, null)));
    assertThat(structSchema.getFields().get().get(1).getName(), equalTo("l2integer"));
    assertThat(
        structSchema.getFields().get().get(1).getSchema(),
        equalTo(
            new SchemaInfo(SchemaInfo.Type.INTEGER, null, null)));

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
                SchemaInfo.Type.MAP,
                null,
                new SchemaInfo(SchemaInfo.Type.STRING, null, null))));

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
                SchemaInfo.Type.ARRAY,
                null,
                new SchemaInfo(SchemaInfo.Type.STRING, null, null))));

    shouldSerializeCorrectly(descriptionString, deserialized);
  }
}
