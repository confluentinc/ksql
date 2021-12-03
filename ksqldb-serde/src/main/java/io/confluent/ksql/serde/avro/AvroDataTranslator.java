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

import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
public class AvroDataTranslator extends ConnectDataTranslator {

  private final Schema ksqlSchema;

  AvroDataTranslator(
      final Schema schema,
      final String schemaFullName
  ) {
    super(AvroSchemas.getAvroCompatibleConnectSchema(schema, schemaFullName));

    this.ksqlSchema = AvroUtil
        .throwOnInvalidSchema(Objects.requireNonNull(schema, "schema"));
  }

  AvroDataTranslator(
      final Schema schema,
      final Schema physicalSchema
  ) {
    super(schema, physicalSchema);
    this.ksqlSchema = AvroUtil
        .throwOnInvalidSchema(Objects.requireNonNull(schema, "schema"));
  }

  @Override
  public Object toKsqlRow(final Schema connectSchema, final Object connectObject) {
    final Object avroCompatibleRow = super.toKsqlRow(connectSchema, connectObject);
    if (avroCompatibleRow == null) {
      return null;
    }

    // If original data is serialized using physcial schema, don't need to replace back to original
    // schema
    if (getPhyiscalSchema().isPresent()) {
      return avroCompatibleRow;
    }
    return replaceSchema(ksqlSchema, avroCompatibleRow);
  }

  @Override
  protected Object toPhysicalSchemaRow(final Object ksqlData, final Schema schema) {
    if (!(ksqlData instanceof Struct)) {
      return ksqlData;
    }
    final Struct struct = new Struct(schema);
    final Struct originalData = (Struct) ksqlData;
    final Schema originalSchema = originalData.schema();

    for (final Field field : schema.fields()) {
      final Optional<Field> originalField = originalSchema.fields().stream().filter(f -> field.name().equals(f.name())).findFirst();
      if (originalField.isPresent()) {
        struct.put(field, originalData.get(originalField.get()));
      } else {
        if (field.schema().defaultValue() != null || field.schema().isOptional()) {
          struct.put(field, field.schema().defaultValue());
        } else {
          throw new KsqlException("Missing default value for required Avro field: [" + field.name()
              + "]. This field appears in Avro schema in Schema Registry");
        }
      }
    }

    return struct;
  }

  @Override
  protected Object toOriginalSchemaRow(final Object ksqlData, final Schema schema) {
    return replaceSchema(schema, ksqlData);
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
      case BYTES:
        if (DecimalUtil.isDecimal(schema)) {
          return DecimalUtil.ensureFit((BigDecimal) object, schema);
        } else {
          return object;
        }
      default:
        return object;
    }
  }
}
