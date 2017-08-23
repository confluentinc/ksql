/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SchemaMapper {
  private final Map<String, ?> configs;

  public SchemaMapper() {
    this(Collections.emptyMap());
  }

  public SchemaMapper(Map<String, ?> configs) {
    this.configs = configs;
  }

  public ObjectMapper registerToObjectMapper(ObjectMapper objectMapper) {
    return objectMapper
        .registerModule(new SimpleModule()
            .addSerializer(Schema.class, new SchemaJsonSerializer(createNewConverter()))
            .addDeserializer(Schema.class, new SchemaJsonDeserializer(createNewConverter())))
        .addMixIn(Field.class, FieldMixin.class);
  }

  private JsonConverter createNewConverter() {
    JsonConverter result = new JsonConverter();
    result.configure(configs, false);
    return result;
  }

  private static class SchemaJsonSerializer extends JsonSerializer<Schema> {
    private final JsonConverter jsonConverter;

    public SchemaJsonSerializer(JsonConverter jsonConverter) {
      this.jsonConverter = jsonConverter;
    }

    @Override
    public void serialize(
        Schema schema,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider
    ) throws IOException {
      jsonGenerator.writeTree(jsonConverter.asJsonSchema(schema));
    }
  }

  private static class SchemaJsonDeserializer extends JsonDeserializer<Schema> {
    private final JsonConverter jsonConverter;

    public SchemaJsonDeserializer(JsonConverter jsonConverter) {
      this.jsonConverter = jsonConverter;
    }

    @Override
    public Schema deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
      return jsonConverter.asConnectSchema(jsonParser.readValueAsTree());
    }
  }

  private static class FieldMixin {
    @JsonProperty("name") private String name;
    @JsonProperty("index") private int index;
    @JsonProperty("schema") private Schema schema;

    @JsonCreator
    public FieldMixin(
        @JsonProperty("name") String name,
        @JsonProperty("index") int index,
        @JsonProperty("schema") Schema schema
    ) {

    }
  }
}