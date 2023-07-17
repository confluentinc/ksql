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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.util.CoercionUtil;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.schema.ksql.SqlBooleans;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  private static final ImmutableMap<Class<?>, Function<String, ?>> STRING_PARSERS = ImmutableMap
      .of(
          Boolean.class, InListEvaluator::stringToBoolean,
          Integer.class, Integer::valueOf,
          Long.class, Long::valueOf,
          Double.class, Double::valueOf,
          BigDecimal.class, BigDecimal::new
      );

  private static final ImmutableMap<
      Class<? extends Number>,
      BiFunction<Number, Number, ? extends Number>
      > WIDENERS = ImmutableMap
      .<Class<? extends Number>, BiFunction<Number, Number, ? extends Number>>builder()
      .put(Integer.class, (v, t) -> v.intValue())
      .put(Long.class, (v, t) -> v.longValue())
      .put(Double.class, (v, t) -> v.doubleValue())
      .put(BigDecimal.class, InListEvaluator::numberToDecimalValue)
      .build();

  private static final ImmutableList<Handler> MATCHERS = ImmutableList.<Handler>builder()
      .add(InListEvaluator::nullsNeverMatch)
      .add(handler(List.class, InListEvaluator::arraysMatch))
      .add(handler(Map.class, InListEvaluator::mapsMatch))
      .add(handler(Struct.class, InListEvaluator::structsMatch))
      .add(InListEvaluator::exactMatch)
      .add(handler(String.class, Object.class, InListEvaluator::parsedMatches))
      .add(handler(Object.class, String.class, InListEvaluator::parsedMatches))
      .add(handler(Number.class, Number.class, InListEvaluator::numbersMatch))
      .build();

  private InListEvaluator() {
  }

  /**
   * Preprocess the list of possible expression, ensuring compatible types, performing literal
   * coercion and removing null literals (which can never match).
   *
   * @param predicate The predicate to process
   * @param typeManager the type manager for the predicate
   * @param lambdaTypeMapping mapping of lambda variables to type
   * @return {@code predicate} after processing.
   */
  public static InPredicate preprocess(
      final InPredicate predicate,
      final ExpressionTypeManager typeManager,
      final Map<String, SqlType> lambdaTypeMapping
  ) {
    final List<Expression> nonNull = ImmutableList.<Expression>builder()
        .add(predicate.getValue())
        .addAll(predicate.getValueList().getValues().stream()
            .filter(e -> !(e instanceof NullLiteral))
            .collect(Collectors.toList()))
        .build();

    final List<Expression> coerced = CoercionUtil
        .coerceUserList(nonNull, typeManager, lambdaTypeMapping)
        .expressions();

    return new InPredicate(
        predicate.getLocation(),
        coerced.get(0),
        new InListExpression(
            predicate.getValueList().getLocation(),
            coerced.subList(1, coerced.size())
        )
    );
  }

  /**
   * Looks for {@code value} in {@code values}.
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
    for (final Handler handler : MATCHERS) {
      final Optional<Boolean> result = handler.accept(requiredValue, possibleMatch);
      if (result.isPresent()) {
        return result.get();
      }
    }

    return false;
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private static boolean isStructuredMatch(
      final Object requiredValue,
      final Object possibleMatch
  ) {
    if (requiredValue == null && possibleMatch == null) {
      // While SQL null is not equal to another null,
      // Struct fields, Map keys and values, and Array elements are considered equal if both null:
      return true;
    }

    return isMatch(requiredValue, possibleMatch);
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

  private static boolean parsedMatches(
      final String requiredValue,
      final Object possibleMatch
  ) {
    return parsedMatches(possibleMatch, requiredValue);
  }

  private static boolean parsedMatches(
      final Object requiredValue,
      final String possibleMatch
  ) {
    final Object parsed = parseString(possibleMatch, requiredValue.getClass());
    return requiredValue.equals(parsed);
  }

  private static boolean numbersMatch(
      final Number requiredValue,
      final Number possibleMatch
  ) {
    final Number possible = possibleMatch.getClass().equals(requiredValue.getClass())
        ? possibleMatch
        : WIDENERS.get(requiredValue.getClass()).apply(possibleMatch, requiredValue);

    return requiredValue.equals(possible);
  }

  private static Object stringToBoolean(final String value) {
    return SqlBooleans.parseBooleanExact(value.trim())
        .map(Object.class::cast)
        .orElse(NO_MATCH);
  }

  private static boolean arraysMatch(final List<?> requiredValue, final List<?> possibleMatch) {
    final Iterator<?> rIt = requiredValue.iterator();
    final Iterator<?> pIt = possibleMatch.iterator();

    while (rIt.hasNext() && pIt.hasNext()) {
      final Object rNext = rIt.next();
      final Object pNext = pIt.next();

      if (!isStructuredMatch(rNext, pNext)) {
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

      if (!isStructuredMatch(rEntry.getValue(), pValue)) {
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
      final Object pValue = possibleMatch.get(rField.name());

      if (!isStructuredMatch(rValue, pValue)) {
        return false;
      }
    }

    return true;
  }

  public static BigDecimal numberToDecimalValue(final Number toConvert, final Number toMatch) {
    return new BigDecimal(toConvert.toString())
        .setScale(((BigDecimal) toMatch).scale(), RoundingMode.UNNECESSARY);
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
