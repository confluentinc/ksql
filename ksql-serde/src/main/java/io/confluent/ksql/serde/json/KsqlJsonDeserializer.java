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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlJsonDeserializer implements Deserializer<Object> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);
  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(word -> false);
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  private static final Schema STRING_ARRAY = SchemaBuilder
      .array(Schema.OPTIONAL_STRING_SCHEMA).build();

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

  private final PersistenceSchema physicalSchema;
  private final boolean isJsonSchema;
  private String target = "?";

  public KsqlJsonDeserializer(
      final PersistenceSchema physicalSchema,
      final boolean isJsonSchema
  ) {
    this.physicalSchema = JsonSerdeUtils.validateSchema(physicalSchema);
    this.isJsonSchema = isJsonSchema;
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean isKey) {
    this.target = isKey ? "key" : "value";
  }

  @Override
  public Object deserialize(final String topic, final byte[] bytes) {
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
          new JsonValueContext(value, physicalSchema.serializedSchema())
      );

      if (LOG.isTraceEnabled()) {
        LOG.trace("Deserialized {}. topic:{}, row:{}", target, topic, coerced);
      }

      return coerced;
    } catch (final Exception e) {
      throw new SerializationException(
          "mvn " + target + " from topic: " + topic, e);
    }
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
    final BigDecimal decimal;
    final boolean isDecimal = DecimalUtil.isDecimal(context.schema);
    if (isDecimal && context.val instanceof NumericNode) {
      decimal = context.val.decimalValue();
      DecimalUtil.ensureFit(decimal, context.schema);
      return decimal;
    } else if (isDecimal && context.val instanceof TextNode) {
      decimal = new BigDecimal(context.val.textValue());
      DecimalUtil.ensureFit(decimal, context.schema);
      return decimal;
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