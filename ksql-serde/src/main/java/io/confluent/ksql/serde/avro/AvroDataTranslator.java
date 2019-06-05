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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;


public class AvroDataTranslator implements DataTranslator {

  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema avroCompatibleSchema;

  AvroDataTranslator(
      final Schema ksqlSchema,
      final String schemaFullName,
      final boolean useNamedMaps
  ) {
    this.ksqlSchema = Objects.requireNonNull(ksqlSchema, "ksqlSchema");
    this.avroCompatibleSchema = buildAvroCompatibleSchema(
        ksqlSchema,
        useNamedMaps,
        new TypeNameGenerator(Collections.singleton(schemaFullName)));
    this.innerTranslator = new ConnectDataTranslator(avroCompatibleSchema);
  }

  Schema getConnectSchema() {
    return avroCompatibleSchema;
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object avroCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    if (avroCompatibleRow == null) {
      return null;
    }

    return convert((Struct)avroCompatibleRow, ksqlSchema);
  }

  @Override
  public Object toConnectRow(final Object struct) {
    final Struct compatibleStruct = convert((Struct)struct, avroCompatibleSchema);
    return innerTranslator.toConnectRow(compatibleStruct);
  }

  private static Struct convert(
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

  private static String avroCompatibleFieldName(final Field field) {
    // Currently the only incompatible field names expected are fully qualified
    // column identifiers. Once quoted identifier support is introduced we will
    // need to implement something more generic here.
    return field.name().replace(".", "_");
  }

  private static Schema buildAvroCompatibleSchema(
      final Schema schema,
      final boolean useNamedMaps,
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
              buildAvroCompatibleSchema(
                  f.schema(), useNamedMaps, typeNameGenerator.with(f.name())));
        }
        break;
      case ARRAY:
        schemaBuilder = SchemaBuilder.array(
            buildAvroCompatibleSchema(
                schema.valueSchema(), useNamedMaps, typeNameGenerator));
        break;
      case MAP:
        final SchemaBuilder mapSchemaBuilder = SchemaBuilder.map(
            buildAvroCompatibleSchema(schema.keySchema(),
                useNamedMaps,
                typeNameGenerator.with(TypeNameGenerator.MAP_KEY_NAME)),
            buildAvroCompatibleSchema(schema.valueSchema(),
                useNamedMaps,
                typeNameGenerator.with(TypeNameGenerator.MAP_VALUE_NAME))
        );
        schemaBuilder = useNamedMaps
          ? mapSchemaBuilder.name(typeNameGenerator.name()) : mapSchemaBuilder;
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
