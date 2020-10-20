/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Used in the code generation to evaluate SQL 'IN (a, b, c)' expressions.
 *
 * <p>For tests, see {@code in.json} QTT test.
 *
 * @see io.confluent.ksql.execution.expression.tree.InPredicate
 */
public final class InListEvaluator {

  private static final Object NO_MATCH = new Object();

  private static final ImmutableList<Handler> HANDLERS = ImmutableList.<Handler>builder()
      .add(InListEvaluator::nullsNeverMatch)
      .add(handler(List.class, InListEvaluator::arraysMatch))
      .add(handler(Map.class, InListEvaluator::mapsMatch))
      .add(handler(Struct.class, InListEvaluator::structsMatch))
      .add(InListEvaluator::exactMatch)
      .add(handler(String.class, Object.class, InListEvaluator::requiredParsedMatches))
      .add(handler(Object.class, String.class, InListEvaluator::possibleParsedMatches))
      .add(handler(Long.class, Integer.class, InListEvaluator::upCastInt))
      .add(handler(Double.class, BigDecimal.class, InListEvaluator::downCastDecimal))
      .build();

  private static final ImmutableMap<Class<?>, Function<String, ?>> STRING_PARSERS = ImmutableMap
      .<Class<?>, Function<String, ?>>builder()
      .put(Boolean.class, InListEvaluator::stringToBoolean)
      .put(Integer.class, Integer::valueOf)
      .put(Long.class, Long::valueOf)
      .put(Double.class, Double::valueOf)
      .put(BigDecimal.class, BigDecimal::new)
      .build();

  private InListEvaluator() {
  }

  /**
   * Looks for {@code value} in {@code values}.
   *
   * <p>SQL NULLs never match.
   *
   * <p>Conversion from INT to BIGINT is handled.
   *
   * <p>Conversion from DECIMAL to DOUBLE is handled.
   *
   * <p>Conversion between STRING and other primitive keys is handled.
   *
   * <p>Invalid conversions are ignored.
   *
   * @param value the value to look for
   * @param values the values to look in
   * @return {@code true} if {@code value} is in {@code values}.
   */
  public static boolean matches(final Object value, final Object... values) {
    if (value == null) {
      // SQL NULL never matches anything:
      return false;
    }

    for (final Object v : values) {
      if (v == null) {
        // SQL NULL never matches anything:
        continue;
      }

      if (isMatch(value, v)) {
        return true;
      }
    }

    return false;
  }

  private static boolean isMatch(final Object requiredValue, final Object possibleMatch) {
    for (final Handler handler : HANDLERS) {
      final Optional<Boolean> result = handler.accept(requiredValue, possibleMatch);
      if (result.isPresent()) {
        return result.get();
      }
    }

    return false;
  }

  private static Optional<Boolean> nullsNeverMatch(
      final Object requiredValue,
      final Object possibleMatch
  ) {
    if (requiredValue == null || possibleMatch == null) {
      // SQL NULL never matches anything:
      return Optional.of(false);
    }

    return Optional.empty();
  }

  private static Optional<Boolean> exactMatch(
      final Object requiredValue,
      final Object possibleMatch
  ) {
    if (requiredValue.equals(possibleMatch)) {
      return Optional.of(true);
    }

    return Optional.empty();
  }

  private static Object parseString(
      final String value,
      final Class<?> requiredType
  ) {
    final Function<String, ?> parser = STRING_PARSERS.get(requiredType);
    if (parser == null) {
      return NO_MATCH;
    }

    try {
      return parser.apply(value);
    } catch (final NumberFormatException e) {
      return NO_MATCH;
    }
  }

  private static boolean requiredParsedMatches(
      final String requiredValue,
      final Object possibleMatch
  ) {
    return possibleParsedMatches(possibleMatch, requiredValue);
  }

  private static boolean possibleParsedMatches(
      final Object requiredValue,
      final String possibleMatch
  ) {
    final Object parsed = parseString(possibleMatch, requiredValue.getClass());
    return requiredValue.equals(parsed);
  }

  private static boolean upCastInt(
      final Long requiredValue,
      final Integer possibleMatch
  ) {
    return requiredValue.equals(possibleMatch.longValue());
  }

  private static boolean downCastDecimal(
      final Double requiredValue,
      final BigDecimal possibleMatch
  ) {
    final double d = possibleMatch.doubleValue();
    if (!new BigDecimal(String.valueOf(d)).equals(possibleMatch)) {
      // Lossy conversion:
      return false;
    }

    return requiredValue.equals(d);
  }


  private static Object stringToBoolean(final String value) {
    switch (value.toUpperCase()) {
      case "T":
      case "TRUE":
        return true;

      case "F":
      case "FALSE":
        return false;

      default:
        return NO_MATCH;
    }
  }

  private static boolean arraysMatch(final List<?> requiredValue, final List<?> possibleMatch) {
    final Iterator<?> rIt = requiredValue.iterator();
    final Iterator<?> pIt = possibleMatch.iterator();

    while (rIt.hasNext() && pIt.hasNext()) {
      final Object rNext = rIt.next();
      final Object pNext = pIt.next();

      if (!isMatch(rNext, pNext)) {
        return false;
      }
    }

    return !rIt.hasNext() && !pIt.hasNext();
  }

  private static boolean mapsMatch(final Map<?, ?> requiredValue, final Map<?, ?> possibleMatch) {
    if (requiredValue.size() != possibleMatch.size()) {
      return false;
    }

    for (final Entry<?, ?> rEntry : requiredValue.entrySet()) {
      final Object pValue = possibleMatch.get(rEntry.getKey());

      if (!isMatch(rEntry.getValue(), pValue)) {
        return false;
      }
    }

    return true;
  }

  private static boolean structsMatch(final Struct requiredValue, final Struct possibleMatch) {
    final Schema rSchema = requiredValue.schema();
    final Schema pSchema = possibleMatch.schema();
    if (rSchema.fields().size() != pSchema.fields().size()) {
      return false;
    }

    for (final Field rField : rSchema.fields()) {
      final Object rValue = requiredValue.get(rField);
      final Object pValue = possibleMatch.get(rField);

      if (!isMatch(rValue, pValue)) {
        return false;
      }
    }

    return true;
  }

  private static <T> Handler handler(final Class<T> type, final TypeSafeHandler<T, T> handler) {
    return handler(type, type, handler);
  }

  private static <T0, T1> Handler handler(
      final Class<T0> requiredType,
      final Class<T1> possibleType,
      final TypeSafeHandler<T0, T1> handler
  ) {
    return (requiredValue, possibleMatch) -> {
      if (!requiredType.isAssignableFrom(requiredValue.getClass())
          || !possibleType.isAssignableFrom(possibleMatch.getClass())) {
        return Optional.empty();
      }
      return Optional.of(handler
          .accept(requiredType.cast(requiredValue), possibleType.cast(possibleMatch)));
    };
  }

  private interface TypeSafeHandler<T0, T1> {

    boolean accept(T0 requiredValue, T1 possibleMatch);
  }

  private interface Handler {

    Optional<Boolean> accept(Object requiredValue, Object possibleMatch);
  }
}
