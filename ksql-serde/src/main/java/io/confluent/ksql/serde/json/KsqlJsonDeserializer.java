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

package io.confluent.ksql.serde.json;

import com.google.gson.Gson;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlJsonDeserializer implements Deserializer<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);

  private final Gson gson;
  private final PersistenceSchema physicalSchema;
  private final JsonConverter jsonConverter;
  private final ProcessingLogger recordLogger;

  KsqlJsonDeserializer(
      final PersistenceSchema physicalSchema,
      final ProcessingLogger recordLogger
  ) {
    this.gson = new Gson();
    this.physicalSchema = JsonSerdeUtils.validateSchema(physicalSchema);
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
    this.recordLogger = Objects.requireNonNull(recordLogger, "recordLogger");
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public Object deserialize(final String topic, final byte[] bytes) {
    try {
      final Object value = deserialize(bytes);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Deserialized value. topic:{}, row:{}", topic, value);
      }
      return value;
    } catch (final Exception e) {
      recordLogger.error(
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(
              e,
              Optional.ofNullable(bytes))
      );
      throw new SerializationException(
          "Error deserializing JSON message from topic: " + topic, e);
    }
  }

  private Object deserialize(final byte[] bytes) {
    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", bytes);
    return enforceFieldType(physicalSchema.getConnectSchema(), schemaAndValue.value(), true);
  }

  private Object enforceFieldType(
      final Schema schema,
      final Object columnVal,
      final boolean topLevel
  ) {
    if (columnVal == null) {
      return null;
    }
    switch (schema.type()) {
      case BOOLEAN:
        return JsonSerdeUtils.toBoolean(columnVal);
      case INT32:
        return JsonSerdeUtils.toInteger(columnVal);
      case INT64:
        return JsonSerdeUtils.toLong(columnVal);
      case FLOAT64:
        return JsonSerdeUtils.toDouble(columnVal);
      case STRING:
        return processString(columnVal);
      case ARRAY:
        return enforceElementTypeForArray(schema, columnVal);
      case MAP:
        return enforceKeyAndValueTypeForMap(schema, columnVal);
      case STRUCT:
        return enforceFieldTypesForStruct(schema, columnVal, topLevel);
      default:
        throw new KsqlException("Type is not supported: " + schema.type());
    }
  }

  private String processString(final Object columnVal) {
    if (columnVal instanceof Map) {
      return gson.toJson(columnVal);
    }
    return columnVal.toString();
  }

  private List<?> enforceElementTypeForArray(final Schema schema, final Object value) {
    if (!(value instanceof List)) {
      throw invalidConversionException(value, schema);
    }

    final List<?> list = (List<?>) value;
    final List<Object> array = new ArrayList<>(list.size());
    for (final Object item : list) {
      array.add(enforceFieldType(schema.valueSchema(), item, false));
    }
    return array;
  }

  private Map<String, Object> enforceKeyAndValueTypeForMap(final Schema schema,
      final Object value) {
    if (!(value instanceof Map)) {
      throw invalidConversionException(value, schema);
    }

    final Map<?, ?> map = (Map<?, ?>) value;
    final Map<String, Object> ksqlMap = new HashMap<>(map.size());
    for (final Map.Entry<?, ?> e : map.entrySet()) {
      ksqlMap.put(
          enforceFieldType(Schema.OPTIONAL_STRING_SCHEMA, e.getKey(), false).toString(),
          enforceFieldType(schema.valueSchema(), e.getValue(), false)
      );
    }
    return ksqlMap;
  }

  @SuppressWarnings("unchecked")
  private Struct enforceFieldTypesForStruct(
      final Schema schema,
      final Object value,
      final boolean topLevel
  ) {
    if (!(value instanceof Map)) {
      throw invalidConversionException(value, schema);
    }

    final Struct columnStruct = new Struct(schema);
    final Map<String, ?> fields = toCaseInsensitiveFieldNameMap((Map<String, ?>) value, topLevel);

    schema.fields().forEach(
        field -> {
          final Object fieldValue = fields.get(field.name().toUpperCase());
          final Object coerced = enforceFieldType(field.schema(), fieldValue, false);
          columnStruct.put(field.name(), coerced);
        });

    return columnStruct;
  }

  private static Map<String, ?> toCaseInsensitiveFieldNameMap(
      final Map<String, ?> map,
      final boolean omitAt
  ) {
    final Map<String, Object> result = new HashMap<>(map.size());
    for (final Map.Entry<String, ?> entry : map.entrySet()) {
      if (omitAt && entry.getKey().startsWith("@")) {
        if (entry.getKey().length() == 1) {
          throw new KsqlException("Field name cannot be '@'.");
        }
        result.put(entry.getKey().toUpperCase().substring(1), entry.getValue());
      } else {
        result.put(entry.getKey().toUpperCase(), entry.getValue());
      }
    }
    return result;
  }

  @Override
  public void close() {
  }

  private static IllegalArgumentException invalidConversionException(
      final Object value,
      final Schema schema
  ) {
    throw JsonSerdeUtils.invalidConversionException(
        value,
        SqlSchemaFormatter.DEFAULT.format(schema)
    );
  }
}
