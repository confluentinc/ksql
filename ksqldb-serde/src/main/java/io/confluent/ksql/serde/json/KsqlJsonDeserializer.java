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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlJsonDeserializer<T> implements Deserializer<T> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);
  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(word -> false);
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
      .registerModule(new SimpleModule().addSerializer(java.sql.Time.class, new DateSerializer()));

  private static final Schema STRING_ARRAY = SchemaBuilder
      .array(Schema.OPTIONAL_STRING_SCHEMA).build();

  private static final Map<Schema.Type, Function<JsonValueContext, Object>> HANDLERS = ImmutableMap
      .<Schema.Type, Function<JsonValueContext, Object>>builder()
      .put(Type.BOOLEAN, context -> JsonSerdeUtils.toBoolean(context.val))
      .put(Type.INT32, KsqlJsonDeserializer::handleInt)
      .put(Type.INT64, KsqlJsonDeserializer::handleLong)
      .put(Type.FLOAT64, context -> JsonSerdeUtils.toDouble(context.val))
      .put(Type.STRING, KsqlJsonDeserializer::processString)
      .put(Type.ARRAY, KsqlJsonDeserializer::enforceElementTypeForArray)
      .put(Type.MAP, KsqlJsonDeserializer::enforceKeyAndValueTypeForMap)
      .put(Type.STRUCT, KsqlJsonDeserializer::enforceFieldTypesForStruct)
      .put(Type.BYTES, KsqlJsonDeserializer::enforceValidBytes)
      .build();

  private final Schema schema;
  private final boolean isJsonSchema;
  private final Class<T> targetType;
  private String target = "?";

  KsqlJsonDeserializer(
      final Schema schema,
      final boolean isJsonSchema,
      final Class<T> targetType
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.isJsonSchema = isJsonSchema;
    this.targetType = Objects.requireNonNull(targetType, "targetType");

    SerdeUtils.throwOnSchemaJavaTypeMismatch(schema, targetType);
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    this.target = isKey ? "key" : "value";
  }

  @Override
  public T deserialize(final String topic, final byte[] bytes) {
    try {
      if (bytes == null) {
        return null;
      }

      // don't use the JsonSchemaConverter to read this data because
      // we require that the MAPPER enables USE_BIG_DECIMAL_FOR_FLOATS,
      // which is not currently available in the standard converters
      final JsonNode value = isJsonSchema
          ? JsonSerdeUtils.readJsonSR(bytes, MAPPER, JsonNode.class)
          : MAPPER.readTree(bytes);

      final Object coerced = enforceFieldType(
          "$",
          new JsonValueContext(value, schema)
      );

      if (LOG.isTraceEnabled()) {
        LOG.trace("Deserialized {}. topic:{}, row:{}", target, topic, coerced);
      }

      return SerdeUtils.castToTargetType(coerced, targetType);
    } catch (final Exception e) {
      // Clear location in order to avoid logging data, for security reasons
      if (e instanceof JsonParseException) {
        ((JsonParseException) e).clearLocation();
      }

      throw new SerializationException(
          "Failed to deserialize " + target + " from topic: " + topic + ". " + e.getMessage(), e);
    }
  }

  public static ObjectReader jsonReader() {
    return MAPPER.reader();
  }

  private static Object enforceFieldType(
      final String pathPart,
      final JsonValueContext context
  ) {
    if (context.val == null || context.val instanceof NullNode) {
      return null;
    }

    try {
      final Function<JsonValueContext, Object> handler = HANDLERS.getOrDefault(
          context.schema.type(),
          type -> {
            throw new KsqlException("Type is not supported: " + type);
          });
      return handler.apply(context);
    } catch (final CoercionException e) {
      throw new CoercionException(e.getRawMessage(), pathPart + e.getPath(), e);
    } catch (final Exception e) {
      throw new CoercionException(e.getMessage(), pathPart, e);
    }
  }

  private static Object handleInt(final JsonValueContext context) {
    if (context.schema.name() == Time.LOGICAL_NAME) {
      return JsonSerdeUtils.toTime(context.val);
    } else if (context.schema.name() == Date.LOGICAL_NAME) {
      return JsonSerdeUtils.toDate(context.val);
    } else {
      return JsonSerdeUtils.toInteger(context.val);
    }
  }

  private static Object handleLong(final JsonValueContext context) {
    if (context.schema.name() == Timestamp.LOGICAL_NAME) {
      return JsonSerdeUtils.toTimestamp(context.val);
    } else {
      return JsonSerdeUtils.toLong(context.val);
    }
  }

  private static String processString(final JsonValueContext context) {
    if (context.val instanceof ObjectNode) {
      try {
        return MAPPER.writeValueAsString(MAPPER.treeToValue(context.val, Object.class));
      } catch (final JsonProcessingException e) {
        throw new KsqlException("Unexpected inability to write value as string: " + context.val);
      }
    }
    if (context.val instanceof ArrayNode) {
      return enforceElementTypeForArray(new JsonValueContext(context.val, STRING_ARRAY)).stream()
          .map(Objects::toString)
          .collect(Collectors.joining(", ", "[", "]"));
    }
    return context.val.asText();
  }

  private static Object enforceValidBytes(final JsonValueContext context) {
    if (DecimalUtil.isDecimal(context.schema)) {
      if (context.val instanceof NumericNode) {
        return DecimalUtil.ensureFit(context.val.decimalValue(), context.schema);
      }

      if (context.val instanceof TextNode) {
        return DecimalUtil.ensureFit(new BigDecimal(context.val.textValue()), context.schema);
      }
    } else if (context.val.isTextual()) {
      try {
        return ByteBuffer.wrap(context.val.binaryValue());
      } catch (IOException e) {
        throw new IllegalArgumentException("Value is not a valid Base64 encoded string: "
            + context.val.textValue());
      }
    }
    throw invalidConversionException(context.val, context.schema);
  }

  private static List<?> enforceElementTypeForArray(final JsonValueContext context) {
    if (!(context.val instanceof ArrayNode)) {
      throw invalidConversionException(context.val, context.schema);
    }

    int idx = 0;
    final ArrayNode list = (ArrayNode) context.val;
    final List<Object> array = new ArrayList<>(list.size());
    for (final JsonNode item : list) {
      final Object element = enforceFieldType(
          "[" + idx++ + "]",
          new JsonValueContext(item, context.schema.valueSchema())
      );

      array.add(element);
    }
    return array;
  }

  private static Map<String, Object> enforceKeyAndValueTypeForMap(final JsonValueContext context) {
    if (!(context.val instanceof ObjectNode)) {
      throw invalidConversionException(context.val, context.schema);
    }

    final ObjectNode map = (ObjectNode) context.val;
    final Map<String, Object> ksqlMap = new HashMap<>(map.size());
    for (final Iterator<Entry<String, JsonNode>> it = map.fields(); it.hasNext(); ) {
      final Entry<String, JsonNode> e = it.next();

      final String key = (String) enforceFieldType(
          "." + e.getKey() + ".key",
          new JsonValueContext(new TextNode(e.getKey()), Schema.OPTIONAL_STRING_SCHEMA)
      );

      final Object value = enforceFieldType(
          "." + e.getKey() + ".value",
          new JsonValueContext(e.getValue(), context.schema.valueSchema())
      );

      ksqlMap.put(key, value);
    }
    return ksqlMap;
  }

  private static Struct enforceFieldTypesForStruct(final JsonValueContext context) {
    if (!(context.val instanceof ObjectNode)) {
      throw invalidConversionException(context.val, context.schema);
    }

    final Struct columnStruct = new Struct(context.schema);
    final ObjectNode jsonFields = (ObjectNode) context.val;
    final Map<String, JsonNode> upperCasedFields = upperCaseKeys(jsonFields);

    for (final Field ksqlField : context.schema.fields()) {
      // the "case insensitive" strategy leverages that all KSQL fields are internally
      // case sensitive - if they were specified without quotes, then they are upper-cased
      // during parsing. any ksql fields that are case insensitive, therefore, will be matched
      // in this case insensitive field map without modification but the quoted fields will not
      // (unless they were all uppercase to start off with, which is expected to match)
      JsonNode fieldValue = jsonFields.get(ksqlField.name());
      if (fieldValue == null) {
        fieldValue = upperCasedFields.get(ksqlField.name());
      }

      final Object coerced = enforceFieldType(
          "." + ksqlField.name(),
          new JsonValueContext(fieldValue, ksqlField.schema())
      );

      columnStruct.put(ksqlField.name(), coerced);
    }

    return columnStruct;
  }

  private static Map<String, JsonNode> upperCaseKeys(final ObjectNode map) {
    final Map<String, JsonNode> result = new HashMap<>(map.size());
    for (final Iterator<Entry<String, JsonNode>> it = map.fields(); it.hasNext(); ) {
      final Entry<String, JsonNode> entry = it.next();
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

  private static final class JsonValueContext {

    private final Schema schema;
    private final JsonNode val;

    JsonValueContext(
        final JsonNode val,
        final Schema schema
    ) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.val = val;
    }
  }

  private static final class CoercionException extends RuntimeException {

    private final String path;
    private final String message;

    CoercionException(final String message, final String path, final Throwable cause) {
      super(message + ", path: " + path, cause);
      this.message = Objects.requireNonNull(message, "message");
      this.path = Objects.requireNonNull(path, "path");
    }

    public String getRawMessage() {
      return message;
    }

    public String getPath() {
      return path;
    }
  }
}