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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlJsonDeserializer implements Deserializer<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);
  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(word -> false);

  private static final Map<Schema.Type, Function<JsonValueContext, Object>> HANDLERS = ImmutableMap
      .<Schema.Type, Function<JsonValueContext, Object>>builder()
      .put(Type.BOOLEAN, context -> JsonSerdeUtils.toBoolean(context.val))
      .put(Type.INT32, context -> JsonSerdeUtils.toInteger(context.val))
      .put(Type.INT64, context -> JsonSerdeUtils.toLong(context.val))
      .put(Type.FLOAT64, context -> JsonSerdeUtils.toDouble(context.val))
      .put(Type.STRING, KsqlJsonDeserializer::processString)
      .put(Type.ARRAY, KsqlJsonDeserializer::enforceElementTypeForArray)
      .put(Type.MAP, KsqlJsonDeserializer::enforceKeyAndValueTypeForMap)
      .put(Type.STRUCT, KsqlJsonDeserializer::enforceFieldTypesForStruct)
      .put(Type.BYTES, KsqlJsonDeserializer::enforceValidBytes)
      .build();

  private final Gson gson;
  private final PersistenceSchema physicalSchema;
  private final JsonConverter jsonConverter;

  KsqlJsonDeserializer(
      final PersistenceSchema physicalSchema
  ) {
    this.gson = new Gson();
    this.physicalSchema = JsonSerdeUtils.validateSchema(physicalSchema);
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
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
      throw new SerializationException(
          "Error deserializing JSON message from topic: " + topic, e);
    }
  }

  private Object deserialize(final byte[] bytes) {
    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", bytes);
    return enforceFieldType(this, physicalSchema.serializedSchema(), schemaAndValue.value());
  }

  private static Object enforceFieldType(
      final KsqlJsonDeserializer deserializer,
      final Schema schema,
      final Object columnVal
  ) {
    return enforceFieldType(new JsonValueContext(deserializer, schema, columnVal));
  }

  private static Object enforceFieldType(final JsonValueContext context) {
    if (context.val == null) {
      return null;
    }

    final Function<JsonValueContext, Object> handler = HANDLERS.getOrDefault(
        context.schema.type(),
        type -> {
          throw new KsqlException("Type is not supported: " + type);
        });
    return handler.apply(context);
  }

  private static String processString(final JsonValueContext context) {
    if (context.val instanceof Map) {
      return context.deserializer.gson.toJson(context.val);
    }
    return context.val.toString();
  }

  private static Object enforceValidBytes(final JsonValueContext context) {
    // before we implement JSON Decimal support, we need to update Connect
    throw invalidConversionException(context.val, context.schema);
  }

  private static List<?> enforceElementTypeForArray(final JsonValueContext context) {
    if (!(context.val instanceof List)) {
      throw invalidConversionException(context.val, context.schema);
    }

    final List<?> list = (List<?>) context.val;
    final List<Object> array = new ArrayList<>(list.size());
    for (final Object item : list) {
      array.add(enforceFieldType(context.deserializer, context.schema.valueSchema(), item));
    }
    return array;
  }

  private static Map<String, Object> enforceKeyAndValueTypeForMap(final JsonValueContext context) {
    if (!(context.val instanceof Map)) {
      throw invalidConversionException(context.val, context.schema);
    }

    final Map<?, ?> map = (Map<?, ?>) context.val;
    final Map<String, Object> ksqlMap = new HashMap<>(map.size());
    for (final Map.Entry<?, ?> e : map.entrySet()) {
      ksqlMap.put(
          enforceFieldType(
              context.deserializer, Schema.OPTIONAL_STRING_SCHEMA, e.getKey()).toString(),
          enforceFieldType(
              context.deserializer, context.schema.valueSchema(), e.getValue())
      );
    }
    return ksqlMap;
  }

  @SuppressWarnings("unchecked")
  private static Struct enforceFieldTypesForStruct(final JsonValueContext context) {
    if (!(context.val instanceof Map)) {
      throw invalidConversionException(context.val, context.schema);
    }

    final Struct columnStruct = new Struct(context.schema);
    final Map<String, ?> jsonFields = (Map<String, ?>) context.val;

    final Map<String, ?> upperCasedFields = upperCaseKeys(jsonFields);

    for (Field ksqlField : context.schema.fields()) {
      // the "case insensitive" strategy leverages that all KSQL fields are internally
      // case sensitive - if they were specified without quotes, then they are upper-cased
      // during parsing. any ksql fields that are case insensitive, therefore, will be matched
      // in this case insensitive field map without modification but the quoted fields will not
      // (unless they were all uppercase to start off with, which is expected to match)
      final Object fieldValue = ObjectUtils.defaultIfNull(
          jsonFields.get(ksqlField.name()),
          upperCasedFields.get(ksqlField.name()));

      final Object coerced = enforceFieldType(
          context.deserializer,
          ksqlField.schema(),
          fieldValue
      );

      columnStruct.put(ksqlField.name(), coerced);
    }

    return columnStruct;
  }

  private static Map<String, ?> upperCaseKeys(final Map<String, ?> map) {
    final Map<String, Object> result = new HashMap<>(map.size());
    for (final Map.Entry<String, ?> entry : map.entrySet()) {
      // what happens if we have two fields with the same name and different case?
      result.put(entry.getKey().toUpperCase(), entry.getValue());
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
        FORMATTER.format(schema)
    );
  }

  private static class JsonValueContext {

    private final KsqlJsonDeserializer deserializer;
    private final Schema schema;
    private final Object val;

    JsonValueContext(
        final KsqlJsonDeserializer deserializer,
        final Schema schema,
        final Object val
    ) {
      this.deserializer = Objects.requireNonNull(deserializer);
      this.schema = Objects.requireNonNull(schema, "schema");
      this.val = val;
    }
  }
}