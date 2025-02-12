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
import io.confluent.ksql.test.model.TestHeader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Struct;

public final class ExpectedRecordComparator {

  private static final Map<JsonNodeType, BiPredicate<Object, JsonNode>> COMPARATORS =
      ImmutableMap.<JsonNodeType, BiPredicate<Object, JsonNode>>builder()
          .put(JsonNodeType.OBJECT, ExpectedRecordComparator::compareStruct)
          .put(JsonNodeType.ARRAY, ExpectedRecordComparator::compareArray)
          .put(JsonNodeType.NUMBER, ExpectedRecordComparator::compareNumber)
          .put(JsonNodeType.STRING, ExpectedRecordComparator::compareText)
          .put(JsonNodeType.BOOLEAN, ExpectedRecordComparator::compareBoolean)
          .put(JsonNodeType.NULL, ExpectedRecordComparator::compareNull)
          .build();

  private ExpectedRecordComparator() {
  }

  public static boolean matches(
      final Header[] actualHeaders, final List<TestHeader> expectedHeaders) {
    if (actualHeaders.length != expectedHeaders.size()) {
      return false;
    }
    for (int i = 0; i < actualHeaders.length; i++) {
      if (!actualHeaders[i].key().equals(expectedHeaders.get(i).key())
          || !Arrays.equals(actualHeaders[i].value(), expectedHeaders.get(i).value())) {
        return false;
      }
    }
    return true;
  }


  public static boolean matches(final Object actualValue, final JsonNode expectedValue) {
    return comparator(expectedValue).test(actualValue, expectedValue);
  }

  private static boolean compareStruct(final Object actualValue, final JsonNode expectedValue) {
    final ObjectNode expected = (ObjectNode) expectedValue;
    final Function<String, Object> getter;
    final int numFields;

    if (actualValue instanceof Struct) {
      getter = ((Struct) actualValue)::get;
      numFields = ((Struct) actualValue).schema().fields().size();
    } else if (actualValue instanceof Map) {
      getter = ((Map<?, ?>) actualValue)::get;
      numFields = ((Map<?, ?>) actualValue).size();
    } else {
      return false;
    }

    if (numFields != expected.size()) {
      return false;
    }

    final Iterator<Entry<String, JsonNode>> fields = expected.fields();
    while (fields.hasNext()) {
      final Entry<String, JsonNode> field = fields.next();

      Object key = getter.apply(field.getKey());
      if (key == null) {
        key = getter.apply(field.getKey().toUpperCase());
      }

      if (!comparator(field.getValue()).test(key, field.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static boolean compareArray(final Object actualValue, final JsonNode expectedValue) {
    final ArrayNode expected = (ArrayNode) expectedValue;
    if (!(actualValue instanceof List)) {
      return false;
    }

    final List<?> actual = (List<?>) actualValue;
    if (actual.size() != expected.size()) {
      return false;
    }

    final Iterator<JsonNode> elements = expected.elements();

    int i = 0;
    while (elements.hasNext()) {
      final JsonNode el = elements.next();
      if (!comparator(el).test(actual.get(i), el)) {
        return false;
      }
      i++;
    }

    return true;
  }

  private static boolean compareNumber(final Object actualValue, final JsonNode expectedValue) {
    final NumericNode expected = (NumericNode) expectedValue;

    if (actualValue instanceof Integer) {
      return expected.intValue() == (Integer) actualValue;
    }

    if (actualValue instanceof Long) {
      return expected.longValue() == (Long) actualValue;
    }

    if (actualValue instanceof Double) {
      return Math.abs(expected.doubleValue() - (Double) actualValue) < 0.000_001;
    }

    if (actualValue instanceof BigDecimal) {
      return compareDecimal((BigDecimal) actualValue, expected);
    }
    return false;
  }

  private static boolean compareDecimal(final BigDecimal actualValue, final NumericNode expected) {
    if (expected.isInt() || expected.isLong()) {
      // Only match if actual has no decimal places:
      if (actualValue.scale() > 0) {
        return false;
      }

      return expected.longValue() == actualValue.longValueExact();
    }

    if (!expected.isBigDecimal()) {
      // we don't want to risk comparing a BigDecimal with something of lower precision
      return false;
    }

    try {
      return expected.decimalValue().equals(actualValue);
    } catch (final ArithmeticException e) {
      // the scale of the expected value cannot match the scale of the actual value
      // without rounding
      return false;
    }
  }

  private static boolean compareText(final Object actualValue, final JsonNode expectedValue) {
    final TextNode expected = (TextNode) expectedValue;
    if (actualValue instanceof String) {
      return expected.asText().equals(actualValue);
    }
    if (actualValue instanceof BigDecimal) {
      return new BigDecimal(expected.asText()).equals(actualValue);
    }
    return false;
  }

  private static boolean compareBoolean(final Object actualValue, final JsonNode expectedValue) {
    return actualValue instanceof Boolean && expectedValue.asBoolean() == (Boolean) actualValue;
  }

  private static boolean compareNull(final Object actualValue, final JsonNode expectedValue) {
    return actualValue == null;
  }

  private static BiPredicate<Object, JsonNode> comparator(final JsonNode node) {
    final JsonNodeType type = node == null ? JsonNodeType.NULL : node.getNodeType();
    final BiPredicate<Object, JsonNode> predicate = COMPARATORS.get(type);

    if (predicate == null) {
      throw new IllegalArgumentException(
          "KSQL Testing Tool cannot expect JSON node type: " + type);
    }
    return predicate;
  }

}
