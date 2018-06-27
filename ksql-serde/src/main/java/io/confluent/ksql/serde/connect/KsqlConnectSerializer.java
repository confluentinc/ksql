/**
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
 **/

package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KsqlConnectSerializer implements Serializer<GenericRow> {
  private final Schema schema;
  private final Converter converter;

  public KsqlConnectSerializer(final Schema schema, final Converter converter) {
    this.schema = addNames(schema, new TypeNameGenerator());
    this.converter = converter;
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }

    try {
      final Struct struct = new Struct(schema);
      Streams.forEachPair(
          schema.fields().stream(),
          genericRow.getColumns().stream(),
          (field, value) -> struct.put(field.name(), replaceSchema(field.schema(), value)));
      return converter.fromConnectData(topic, schema, struct);
    } catch (Exception e) {
      throw new SerializationException(
          "Error serializing row to topic " + topic + " using Converter API", e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public void close() {
  }

  private static class TypeNameGenerator {
    private static final String DELIMITER = "_";
    private static final String DEFAULT_SCHEMA_NAME_BASE = "KSQLDefaultSchemaName";

    private Iterable<String> names;

    TypeNameGenerator() {
      this(ImmutableList.of(DEFAULT_SCHEMA_NAME_BASE));
    }

    private TypeNameGenerator(Iterable<String> names) {
      this.names = names;
    }

    TypeNameGenerator with(String name) {
      return new TypeNameGenerator(Iterables.concat(names, ImmutableList.of(name)));
    }

    public String name() {
      return String.join(DELIMITER, names);
    }
  }

  private Schema addNames(final Schema schema, final TypeNameGenerator typeNameGenerator) {
    final SchemaBuilder schemaBuilder;
    switch (schema.type()) {
      default:
        return schema;
      case STRUCT:
        schemaBuilder = SchemaBuilder.struct();
        if (schema.name() == null) {
          schemaBuilder.name(typeNameGenerator.name());
        }
        for (final Field f : schema.fields()) {
          schemaBuilder.field(f.name(), addNames(f.schema(), typeNameGenerator.with(f.name())));
        }
        break;
      case ARRAY:
        schemaBuilder = SchemaBuilder.array(addNames(schema.valueSchema(), typeNameGenerator));
        break;
      case MAP:
        schemaBuilder = SchemaBuilder.map(
            addNames(schema.keySchema(), typeNameGenerator),
            addNames(schema.valueSchema(), typeNameGenerator));
        break;
    }
    if (schema.isOptional()) {
      schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private Object replaceSchema(Schema schema, Object object) {
    if (object == null) {
      return null;
    }
    switch (schema.type()) {
      case ARRAY:
        return ((List) object).stream()
            .map(e -> replaceSchema(schema.valueSchema(), e))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>) object).entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> replaceSchema(schema.keySchema(), e.getKey()),
                    e -> replaceSchema(schema.valueSchema(), e.getValue())
                )
            );

      case STRUCT:
        final Struct struct = new Struct(schema);
        schema.fields().forEach(
            f -> struct.put(
                f.name(),
                replaceSchema(f.schema(), ((Struct) object).get(f.name())))
        );
        return struct;
      default:
        return object;
    }
  }
}
