package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SchemaMapper {
  private final JsonConverter jsonConverter;
  private final JsonSerializer<Schema> schemaSerializer;
  private final JsonDeserializer<Schema> schemaDeserializer;
  private final JsonSerializer<Field> fieldSerializer;
  private final JsonDeserializer<Field> fieldDeserializer;

  public SchemaMapper() {
    this(Collections.emptyMap());
  }

  public SchemaMapper(Map<String, ?> configs) {
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(configs, false);

    this.schemaSerializer = new SchemaJsonSerializer();
    this.schemaDeserializer = new SchemaJsonDeserializer();
    this.fieldSerializer = new FieldJsonSerializer();
    this.fieldDeserializer = new FieldJsonDeserializer();
  }

  public JsonSerializer<Schema> getSchemaSerializer() {
    return schemaSerializer;
  }

  public JsonDeserializer<Schema> getSchemaDeserializer() {
    return schemaDeserializer;
  }

  public JsonSerializer<Field> getFieldSerializer() {
    return fieldSerializer;
  }

  public JsonDeserializer<Field> getFieldDeserializer() {
    return fieldDeserializer;
  }

  private class SchemaJsonSerializer extends JsonSerializer<Schema> {
    @Override
    public void serialize(Schema schema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeTree(jsonConverter.asJsonSchema(schema));
    }
  }

  private class SchemaJsonDeserializer extends JsonDeserializer<Schema> {
    @Override
    public Schema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
      return jsonConverter.asConnectSchema(jsonParser.readValueAsTree());
    }
  }

  private class FieldJsonSerializer extends JsonSerializer<Field> {
    @Override
    public void serialize(Field field, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeStartObject();

      jsonGenerator.writeStringField("name", field.name());

      jsonGenerator.writeFieldName("index");
      jsonGenerator.writeNumber(field.index());

      jsonGenerator.writeFieldName("schema");
      jsonGenerator.writeTree(jsonConverter.asJsonSchema(field.schema()));

      jsonGenerator.writeEndObject();
    }
  }

  private class FieldJsonDeserializer extends JsonDeserializer<Field> {
    @Override
    public Field deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      TreeNode treeNode = jsonParser.readValueAsTree();
      if (!treeNode.isObject()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }

      TreeNode nameField = treeNode.get("name");
      if (nameField == null || !nameField.isValueNode()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }
      ValueNode nameValueNode = (ValueNode) nameField;
      if (!nameValueNode.isTextual()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }
      String name = nameValueNode.asText();

      TreeNode indexField = treeNode.get("index");
      if (indexField == null || !indexField.isValueNode()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }
      ValueNode indexValueNode = (ValueNode) indexField;
      if (!indexValueNode.isNumber()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }
      int index = indexValueNode.asInt();

      TreeNode schemaField = treeNode.get("schema");
      if (schemaField == null || !schemaField.isObject()) {
        throw new RuntimeException("Invalid JSON format for serialized Field object");
      }
      Schema schema = jsonConverter.asConnectSchema((ObjectNode) schemaField);

      return new Field(name, index, schema);
    }
  }
}