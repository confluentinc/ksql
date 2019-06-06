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

package io.confluent.ksql.serde.avro;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Translates KSQL data and schemas to Avro equivalents.
 *
 * <p>Responsible for converting the KSQL schema to a version ready for connect to convert to an
 * avro schema.
 *
 * <p>This includes ensuring field names are valid Avro field names and that nested types do not
 * have name clashes.
 */
public class AvroDataTranslator implements DataTranslator {

  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema avroCompatibleSchema;

  AvroDataTranslator(
      final Schema schema,
      final String schemaFullName,
      final boolean useNamedMaps
  ) {
    this.ksqlSchema = throwOnInvalidSchema(Objects.requireNonNull(schema, "schema"));

    this.avroCompatibleSchema = buildAvroCompatibleSchema(
        this.ksqlSchema,
        new TypeNameGenerator(Collections.singleton(schemaFullName), useNamedMaps)
    );
    this.innerTranslator = new ConnectDataTranslator(avroCompatibleSchema);
  }

  Schema getAvroCompatibleSchema() {
    return avroCompatibleSchema;
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object avroCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (avroCompatibleRow == null) {
      return null;
    }

    return replaceSchema(ksqlSchema, avroCompatibleRow);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    final Object compatible = replaceSchema(avroCompatibleSchema, ksqlData);
    return innerTranslator.toConnectRow(compatible);
  }

  private static Struct convertStruct(
      final Struct source,
      final Schema targetSchema
  ) {
    final Struct struct = new Struct(targetSchema);

    final Iterator<Field> sourceIt = source.schema().fields().iterator();

    for (final Field targetField : targetSchema.fields()) {
      final Field sourceField = sourceIt.next();
      final Object value = source.get(sourceField);
      final Object adjusted = replaceSchema(targetField.schema(), value);
      struct.put(targetField, adjusted);
    }

    return struct;
  }

  private static final class TypeNameGenerator {

    private static final String DELIMITER = "_";

    static final String MAP_KEY_NAME = "MapKey";
    static final String MAP_VALUE_NAME = "MapValue";

    private final Iterable<String> names;
    private final boolean useNamedMaps;

    private TypeNameGenerator(final Iterable<String> names, final boolean useNamedMaps) {
      this.names = requireNonNull(names, "names");
      this.useNamedMaps = useNamedMaps;
    }

    TypeNameGenerator with(final String name) {
      return new TypeNameGenerator(Iterables.concat(names, ImmutableList.of(name)), useNamedMaps);
    }

    public String name() {
      return String.join(DELIMITER, names);
    }
  }

  private static String avroCompatibleFieldName(final Field field) {
    // Currently the only incompatible field names expected are fully qualified
    // column identifiers. Once quoted identifier support is introduced we will
    // need to implement something more generic here.
    return field.name().replace(".", "_");
  }

  private static Schema buildAvroCompatibleSchema(
      final Schema schema,
      final TypeNameGenerator typeNameGenerator
  ) {
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
              buildAvroCompatibleSchema(
                  f.schema(), typeNameGenerator.with(f.name())));
        }
        break;
      case ARRAY:
        schemaBuilder = SchemaBuilder.array(
            buildAvroCompatibleSchema(
                schema.valueSchema(), typeNameGenerator));
        break;
      case MAP:
        final SchemaBuilder mapSchemaBuilder = SchemaBuilder.map(
            buildAvroCompatibleSchema(schema.keySchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_KEY_NAME)),
            buildAvroCompatibleSchema(schema.valueSchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_VALUE_NAME))
        );
        schemaBuilder = typeNameGenerator.useNamedMaps
            ? mapSchemaBuilder.name(typeNameGenerator.name())
            : mapSchemaBuilder;
        break;
    }
    if (schema.isOptional()) {
      schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static Object replaceSchema(final Schema schema, final Object object) {
    if (object == null) {
      return null;
    }
    switch (schema.type()) {
      case ARRAY:
        final List<Object> ksqlArray = new ArrayList<>(((List) object).size());
        ((List) object).forEach(
            e -> ksqlArray.add(replaceSchema(schema.valueSchema(), e)));
        return ksqlArray;

      case MAP:
        final Map<Object, Object> ksqlMap = new HashMap<>();
        ((Map<Object, Object>) object).forEach(
            (key, value) -> ksqlMap.put(
                replaceSchema(schema.keySchema(), key),
                replaceSchema(schema.valueSchema(), value)
            ));
        return ksqlMap;

      case STRUCT:
        return convertStruct((Struct) object, schema);

      default:
        return object;
    }
  }


  private static Schema throwOnInvalidSchema(final Schema schema) {

    class SchemaValidator implements Visitor<Void> {

      @Override
      public Void visitMap(final Schema schema, final Void key, final Void value) {
        if (schema.keySchema().type() != Type.STRING) {
          throw new IllegalArgumentException("Avro only supports MAPs with STRING keys");
        }
        return null;
      }

      @Override
      public Void visitSchema(final Schema schema) {
        return null;
      }
    }

    SchemaWalker.visit(schema, new SchemaValidator());
    return schema;
  }
}
