package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class SchemaDescriptionFormatTest {
  ObjectMapper objectMapper = new ObjectMapper();

  private void shouldSerializeCorrectly(String descriptionString,
                                        List<FieldInfo> deserialized) throws IOException {
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    List deserializedGeneric = objectMapper.readValue(descriptionString, List.class);
    String serialized=  objectMapper.writeValueAsString(deserialized);
    assertThat(
        objectMapper.readValue(serialized, List.class),
        equalTo(deserializedGeneric));
  }

  @Test
  public void shouldFormatSchemaEntityCorrectlyForStruct() throws IOException {
    String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"l1integer\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"INTEGER\"\n" +
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
        "            \"type\": \"STRING\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"name\": \"l2integer\",\n" +
        "          \"schema\": {\n" +
        "            \"type\": \"INTEGER\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "]";

    List<FieldInfo> deserialized = objectMapper.readValue(
        descriptionString, new TypeReference<List<FieldInfo>>(){});

    // Test deserialization
    assertThat(deserialized.size(), equalTo(2));
    assertThat(deserialized.get(0).getName(), equalTo("l1integer"));
    assertThat(
        deserialized.get(0).getSchema(),
        equalTo(
            new SchemaInfo(SchemaInfo.Type.INTEGER, null, null)));
    assertThat(deserialized.get(1).getName(), equalTo("l1struct"));
    SchemaInfo structSchema = deserialized.get(1).getSchema();
    assertThat(structSchema.getType(), equalTo(SchemaInfo.Type.STRUCT));
    assertThat(structSchema.getMemberSchema(), nullValue());
    assertThat(structSchema.getFields().size(), equalTo(2));
    assertThat(structSchema.getFields().get(0).getName(), equalTo("l2string"));
    assertThat(
        structSchema.getFields().get(0).getSchema(),
        equalTo(
            new SchemaInfo(SchemaInfo.Type.STRING, null, null)));
    assertThat(structSchema.getFields().get(1).getName(), equalTo("l2integer"));
    assertThat(
        structSchema.getFields().get(1).getSchema(),
        equalTo(
            new SchemaInfo(SchemaInfo.Type.INTEGER, null, null)));

    shouldSerializeCorrectly(descriptionString, deserialized);
  }

  @Test
  public void shouldFormatSchemaEntityCorrectlyForMap() throws IOException {
    String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"mapfield\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"MAP\",\n" +
        "      \"memberSchema\": {\n" +
        "        \"type\": \"STRING\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "]";

    List<FieldInfo> deserialized = objectMapper.readValue(
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
    String descriptionString = "[\n" +
        "  {\n" +
        "    \"name\": \"arrayfield\",\n" +
        "    \"schema\": {\n" +
        "      \"type\": \"ARRAY\",\n" +
        "      \"memberSchema\": {\n" +
        "        \"type\": \"STRING\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "]";

    List<FieldInfo> deserialized = objectMapper.readValue(
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
