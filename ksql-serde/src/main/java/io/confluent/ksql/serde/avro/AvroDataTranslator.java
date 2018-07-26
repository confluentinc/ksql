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

package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import io.confluent.ksql.util.KsqlConstants;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class AvroDataTranslator implements DataTranslator {
  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema avroCompatibleSchema;

  public AvroDataTranslator(final Schema ksqlSchema) {
    this.ksqlSchema = ksqlSchema;
    this.avroCompatibleSchema = buildAvroCompatibleSchema(
        ksqlSchema,
        new TypeNameGenerator());
    this.innerTranslator = new ConnectDataTranslator(avroCompatibleSchema);
  }

  @Override
  public GenericRow toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final GenericRow avroCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (avroCompatibleRow == null) {
      return null;
    }
    final List<Object> columns = new LinkedList<>();
    for (int i = 0; i < avroCompatibleRow.getColumns().size(); i++) {
      columns.add(
          replaceSchema(
              ksqlSchema.fields().get(i).schema(),
              avroCompatibleRow.getColumns().get(i)));
    }
    return new GenericRow(columns);
  }

  @Override
  public Struct toConnectRow(final GenericRow genericRow) {
    final List<Object> columns = new LinkedList<>();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      columns.add(
          replaceSchema(
              avroCompatibleSchema.fields().get(i).schema(),
              genericRow.getColumns().get(i)));
    }
    return innerTranslator.toConnectRow(new GenericRow(columns));
  }

  private static class TypeNameGenerator {
    private static final String DELIMITER = "_";

    static final String MAP_KEY_NAME = "MapKey";
    static final String MAP_VALUE_NAME = "MapValue";

    private Iterable<String> names;

    TypeNameGenerator() {
      this(ImmutableList.of(KsqlConstants.AVRO_SCHEMA_FULL_NAME));
    }

    private TypeNameGenerator(final Iterable<String> names) {
      this.names = names;
    }

    TypeNameGenerator with(final String name) {
      return new TypeNameGenerator(Iterables.concat(names, ImmutableList.of(name)));
    }

    public String name() {
      return String.join(DELIMITER, names);
    }
  }

  private String avroCompatibleFieldName(final Field field) {
    // Currently the only incompatible field names expected are fully qualified
    // column identifiers. Once quoted identifier support is introduced we will
    // need to implement something more generic here.
    return field.name().replace(".", "_");
  }

  private Schema buildAvroCompatibleSchema(final Schema schema,
                                           final TypeNameGenerator typeNameGenerator) {
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
          schemaBuilder.field(
              avroCompatibleFieldName(f),
              buildAvroCompatibleSchema(f.schema(), typeNameGenerator.with(f.name())));
        }
        break;
      case ARRAY:
        schemaBuilder = SchemaBuilder.array(
            buildAvroCompatibleSchema(schema.valueSchema(), typeNameGenerator));
        break;
      case MAP:
        schemaBuilder = SchemaBuilder.map(
            buildAvroCompatibleSchema(schema.keySchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_KEY_NAME)),
            buildAvroCompatibleSchema(schema.valueSchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_VALUE_NAME)));
        break;
    }
    if (schema.isOptional()) {
      schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private Object replaceSchema(final Schema schema, final Object object) {
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
