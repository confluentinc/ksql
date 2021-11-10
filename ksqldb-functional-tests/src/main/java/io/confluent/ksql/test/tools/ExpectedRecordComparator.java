/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public final class ExpectedRecordComparator {

  private static final Map<JsonNodeType, BiFunction<Object, JsonNode, Optional<String>>> COMPARATORS =
      ImmutableMap.<JsonNodeType, BiFunction<Object, JsonNode, Optional<String>>>builder()
          .put(JsonNodeType.OBJECT, ExpectedRecordComparator::compareStruct)
          .put(JsonNodeType.ARRAY, ExpectedRecordComparator::compareArray)
          .put(JsonNodeType.NUMBER, ExpectedRecordComparator::compareNumber)
          .put(JsonNodeType.STRING, ExpectedRecordComparator::compareText)
          .put(JsonNodeType.BOOLEAN, ExpectedRecordComparator::compareBoolean)
          .put(JsonNodeType.NULL, ExpectedRecordComparator::compareNull)
          .build();

  private ExpectedRecordComparator() {
  }

  public static Optional<String> matches(final Object actualValue, final JsonNode expectedValue) {
    return comparator(expectedValue).apply(actualValue, expectedValue);
  }

  private static Optional<String> compareStruct(final Object actualValue, final JsonNode expectedValue) {
    final ObjectNode expected = (ObjectNode) expectedValue;
    final Function<String, Object> getter;
    final Set<?> actualKeys;

    if (actualValue instanceof Struct) {
      getter = ((Struct) actualValue)::get;
      actualKeys = ((Struct) actualValue).schema().fields().stream().map(Field::name).collect(toSet());
    } else if (actualValue instanceof Map) {
      getter = ((Map<?, ?>) actualValue)::get;
      actualKeys = ((Map<?, ?>) actualValue).keySet();
    } else {
      return Optional.of(actualValue + " is not a Struct");
    }

    if (actualKeys.size() != expected.size()) {
      return Optional.of(format("fields %s not match %s", actualKeys, getFieldNames(expected)));
    }

    final Iterator<Entry<String, JsonNode>> fields = expected.fields();
    while (fields.hasNext()) {
      final Entry<String, JsonNode> field = fields.next();

      Object key = getter.apply(field.getKey());
      if (key == null) {
        key = getter.apply(field.getKey().toUpperCase());
      }

      final Optional<String> error = comparator(field.getValue()).apply(key, field.getValue());
      if (error.isPresent()) {
        return Optional.of(format("%s = %s but expected %s (%s)", field.getKey(), key, field.getValue(), error.get()));
      }
    }
    return Optional.empty();
  }

  @NotNull
  private static Set<String> getFieldNames(final ObjectNode expected) {
    return Lists.newArrayList(expected.fields()).stream().map(Entry::getKey).collect(toSet());
  }

  private static Optional<String> compareArray(final Object actualValue, final JsonNode expectedValue) {
    final ArrayNode expected = (ArrayNode) expectedValue;
    if (!(actualValue instanceof List)) {
      return Optional.of(actualValue + " is not an array");
    }

    final List<?> actual = (List<?>) actualValue;
    if (actual.size() != expected.size()) {
      return Optional.of(format("%s has %s instead of expected %s", actualValue, actual.size(), expected.size()));
    }

    final Iterator<JsonNode> elements = expected.elements();

    int i = 0;
    while (elements.hasNext()) {
      final JsonNode el = elements.next();
      final Object value = actual.get(i);
      final Optional<String> error = comparator(el).apply(value, el);
      if (error.isPresent()) {
        return Optional.of(format("%s not match %s: %s", value, el, error.get()));
      }
      i++;
    }

    return Optional.empty();
  }

  private static Optional<String> compareNumber(final Object actualValue, final JsonNode expectedValue) {
    final NumericNode expected = (NumericNode) expectedValue;
    final Optional<String> error = Optional.of(format("%s not match %s", actualValue, expectedValue));

    if (actualValue instanceof Integer) {
      return error.filter(e -> expected.intValue() != (Integer) actualValue);
    }

    if (actualValue instanceof Long) {
      return error.filter(e -> expected.longValue() != (Long) actualValue);
    }

    if (actualValue instanceof Double) {
      return error.filter(e -> expected.doubleValue() != (Double) actualValue);
    }

    if (actualValue instanceof BigDecimal) {
      return compareDecimal((BigDecimal) actualValue, expected);
    }
    return Optional.of(actualValue + " is not a number");
  }

  private static Optional<String> compareDecimal(final BigDecimal actualValue, final NumericNode expected) {
    if (expected.isInt() || expected.isLong()) {
      // Only match if actual has no decimal places:
      if (actualValue.scale() > 0) {
        return Optional.of(format("%s not match %s", actualValue, expected));
      }

      return expected.longValue() == actualValue.longValueExact()
              ? Optional.empty()
              : Optional.of(format("%s not match %s", actualValue, expected));
    }

    if (!expected.isBigDecimal()) {
      // we don't want to risk comparing a BigDecimal with something of lower precision
      return Optional.of(format("%s not match %s", actualValue, expected));
    }

    try {

      return expected.decimalValue().equals(actualValue)
              ? Optional.empty()
              : Optional.of(format("%s not match %s", actualValue, expected));

    } catch (final ArithmeticException e) {
      // the scale of the expected value cannot match the scale of the actual value
      // without rounding
      return Optional.of(format("ArithmeticException in %s", expected));
    }
  }

  private static Optional<String> compareText(final Object actualValue, final JsonNode expectedValue) {
    final TextNode expected = (TextNode) expectedValue;

    if (actualValue instanceof String) {

      return expected.asText().equals(actualValue)
              ? Optional.empty()
              : Optional.of(format("%s not match %s", actualValue, expected));
    }

    if (actualValue instanceof BigDecimal) {

      return new BigDecimal(expected.asText()).equals(actualValue)
              ? Optional.empty()
              : Optional.of(format("%s not match %s", actualValue, expected));
    }

    return Optional.of(format("%s is not a string", actualValue));
  }

  private static Optional<String> compareBoolean(final Object actualValue, final JsonNode expectedValue) {

    if (!(actualValue instanceof Boolean)) {
      return Optional.of(format("%s is not a boolean", actualValue));
    }

    return expectedValue.asBoolean() == (Boolean) actualValue
            ? Optional.empty()
            : Optional.of(format("%s not match %s", actualValue, expectedValue));
  }

  private static Optional<String> compareNull(final Object actualValue, final JsonNode expectedValue) {
    return actualValue == null ? Optional.empty() : Optional.of(format("%s is not null", actualValue));
  }

  private static BiFunction<Object, JsonNode, Optional<String>> comparator(final JsonNode node) {
    final JsonNodeType type = node == null ? JsonNodeType.NULL : node.getNodeType();
    final BiFunction<Object, JsonNode, Optional<String>> predicate = COMPARATORS.get(type);

    if (predicate == null) {
      throw new IllegalArgumentException(
          "KSQL Testing Tool cannot expect JSON node type: " + type);
    }
    return predicate;
  }

}
