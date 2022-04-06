/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

/**
 * Translates KSQL data to and from connect schema conformed data. The connect schema should be
 * translated from ParsedSchema from Schema Registry.
 *
 * <p>The schema may not be compatible with KSQL schema. For example, optional field
 * in KSQL schema might be required in SR schema in which case the translation to connect row will
 * fail.
 */
public class ConnectSRSchemaDataTranslator extends ConnectDataTranslator {
  private RowTranslator rowTranslator;

  public ConnectSRSchemaDataTranslator(final Schema schema, final String formatName) {
    super(schema);

    switch (formatName.trim().toUpperCase()) {
      case AvroFormat.NAME:
      case ProtobufFormat.NAME:
        this.rowTranslator = new AvroAndProtobufRowTranslator();
        break;
      default:
        this.rowTranslator = new DefaultRowTranslator();
    }
  }

  protected void validate(final Schema originalSchema) {
    if (originalSchema.type() != getSchema().type()) {
      return;
    }
    if (originalSchema.type() != Type.STRUCT) {
      return;
    }
    final Schema schema = getSchema();
    for (final Field field : originalSchema.fields()) {
      if (!schema.fields().stream().anyMatch(f -> field.name().equals(f.name()))) {
        throw new KsqlException(
            "Schema from Schema Registry misses field with name: " + field.name());
      }
    }
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    return rowTranslator.translate(ksqlData);
  }

  private interface RowTranslator {
    Object translate(Object ksqlData);
  }

  /**
   * Reconstruct ksqlData struct with given schema and try to put original data in it.
   * Schema may have more fields than ksqlData, don't put those field by default. If needed by
   * some format like Avro, use the AvroAndProtobufRowTranslator
   */
  private class DefaultRowTranslator implements RowTranslator {
    @Override
    public Object translate(final Object ksqlData) {
      if (ksqlData instanceof Struct) {
        validate(((Struct) ksqlData).schema());
        final Schema schema = getSchema();
        final Struct struct = new Struct(schema);
        final Struct source = (Struct) ksqlData;

        for (final Field sourceField : source.schema().fields()) {
          final Object value = source.get(sourceField);
          struct.put(sourceField.name(), value);
        }

        return struct;
      }

      return ksqlData;
    }
  }

  /**
   * Translates KSQL data and schemas to Avro and Protobuf equivalents.
   *
   * <p>Responsible for converting the KSQL schema to a version ready for connect to convert to an
   * avro and protobuf schema.
   *
   * <p>This includes ensuring field names are valid Avro and Protobuf field names and that nested
   * types do not have name clashes.
   */
  private class AvroAndProtobufRowTranslator implements RowTranslator {
    @Override
    public Object translate(final Object ksqlData) {
      if (!(ksqlData instanceof Struct)) {
        return ksqlData;
      }
      final Schema schema = getSchema();
      final Struct struct = new Struct(schema);
      final Struct originalData = (Struct) ksqlData;
      final Schema originalSchema = originalData.schema();

      validate(originalSchema);

      for (final Field field : schema.fields()) {
        final Optional<Field> originalField = originalSchema.fields().stream()
            .filter(f -> field.name().equals(f.name())).findFirst();
        if (originalField.isPresent()) {
          struct.put(field, originalData.get(originalField.get()));
        } else {
          if (field.schema().defaultValue() != null || field.schema().isOptional()) {
            struct.put(field, field.schema().defaultValue());
          } else {
            throw new KsqlException("Missing default value for required field: [" + field.name()
                + "]. This field appears in the schema in Schema Registry");
          }
        }
      }

      return struct;
    }
  }
}
