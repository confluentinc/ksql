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

package io.confluent.ksql.execution.util;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * SQL type coercion helper functions.
 */
@SuppressWarnings("UnstableApiUsage")
public final class CoercionUtil {

  private static final DefaultSqlValueCoercer LITERAL_COERCER = DefaultSqlValueCoercer.LAX;
  private static final DefaultSqlValueCoercer EXPRESSION_COERCER = DefaultSqlValueCoercer.STRICT;

  private static final BigInteger INT_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger INT_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
  private static final BigInteger BIGINT_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger BIGINT_MIN = BigInteger.valueOf(Long.MIN_VALUE);

  private CoercionUtil() {
  }

  // test purposes only
  static Result coerceUserList(
      final Collection<Expression> expressions,
      final ExpressionTypeManager typeManager
  ) {
    return coerceUserList(expressions, typeManager, Collections.emptyMap());
  }

  /**
   * Coerce a list of user-supplied expressions.
   *
   * <p>User supplied expressions lists, such as those used in {@code a IN [b, c, d]} or structured
   * type constructors, can contain literals that need coercing, and expressions that need
   * validating to ensure all conform to a common Sql type.
   *
   * <p>For example, the {@code ARRAY} constructor, when creating a numeric array, can be called
   * with a combination of different numeric types, and even strings containing numbers, such as:
   *
   * <pre>{@code
   * ARRAY[10, 11.2, 13.00, '14.1230', column0, expression0]
   * }</pre>
   *
   * <p>...such a list must have its elements converted to a common type. Where a common type can
   * not be found, an error is thrown. Where found, the common type is the widest type of all
   * expressions in the list. Where widest means:
   *
   * <ul>
   *   <li>for numeric types: the widest numeric type, e.g BIGINT is wider than INTEGER.</li>
   *   <li>for struct types: the super set of all fields.</li>
   * </ul>
   *
   * <p>Any {@link Literal literal} expressions will be coerced to new literals of the common type.
   *
   * <p>Any non-literal expressions that don't match the common type, but which can be coerced, will
   * be wrapped in an explicit {@code CAST} to convert them to the required type.
   *
   * <p>Coercion is performed in order. So the type of the first non-null expression drives the
   * common type. For example, if the first non-null expression is a string, then all other
   * expressions must be coercible to a string. If its numeric, then all other expressions must be
   * coercible to a number, etc.
   *
   * @param expressions the expressions to coerce: the order matters!
   * @param typeManager the type manager used to resolve expression types.
   * @param lambdaTypeMapping mapping of lambda variables to types.
   * @return the coercion result.
   */
  public static Result coerceUserList(
      final Collection<Expression> expressions,
      final ExpressionTypeManager typeManager,
      final Map<String, SqlType> lambdaTypeMapping
  ) {
    return new UserListCoercer(typeManager, lambdaTypeMapping).coerce(expressions);
  }

  public static final class Result {

    private final Optional<SqlType> commonType;
    private final List<Expression> expressions;


    public Result(
        final Optional<SqlType> commonType,
        final List<Expression> expressions
    ) {
      this.commonType = requireNonNull(commonType, "commonType");
      this.expressions = requireNonNull(expressions, "expressions");
    }

    public Optional<SqlType> commonType() {
      return commonType;
    }

    public List<Expression> expressions() {
      return expressions;
    }
  }

  private static final class UserListCoercer {

    private final ExpressionTypeManager typeManager;
    private final Map<String, SqlType> lambdaTypeMapping;

    UserListCoercer(
        final ExpressionTypeManager typeManager,
        final Map<String, SqlType> lambdaTypeMapping
    ) {
      this.typeManager = requireNonNull(typeManager, "typeManager");
      this.lambdaTypeMapping = requireNonNull(lambdaTypeMapping, "lambdaTypeMapping");
    }

    Result coerce(final Collection<Expression> expressions) {
      final List<TypedExpression> typedExpressions = typedExpressions(expressions);

      final Optional<SqlType> commonType = resolveCommonType(typedExpressions);
      if (!commonType.isPresent()) {
        // No elements or only null literal elements:
        return new Result(commonType, ImmutableList.copyOf(expressions));
      }

      final List<Expression> converted = convertToCommonType(typedExpressions, commonType.get());
      return new Result(commonType, converted);
    }

    private List<TypedExpression> typedExpressions(final Collection<Expression> expressions) {
      return expressions.stream()
          .map(e ->
              new TypedExpression(typeManager.getExpressionSqlType(e, lambdaTypeMapping), e))
          .collect(Collectors.toList());
    }

    private Optional<SqlType> resolveCommonType(final List<TypedExpression> expressions) {
      Optional<SqlType> result = Optional.empty();

      for (final TypedExpression e : expressions) {
        result = resolveCommonType(e, result);
      }

      return result;
    }

    private Optional<SqlType> resolveCommonType(
        final TypedExpression exp,
        final Optional<SqlType> commonType
    ) {
      if (!commonType.isPresent()) {
        // First element:
        return exp.type();
      }

      if (!exp.type().isPresent()) {
        // Null literal:
        return commonType;
      }

      final SqlType targetType = commonType.get();

      if (exp.expression() instanceof CreateArrayExpression) {
        return resolveCommonArrayType(exp, targetType);
      }

      if (exp.expression() instanceof CreateMapExpression) {
        return resolveCommonMapType(exp, targetType);
      }

      if (exp.expression() instanceof CreateStructExpression) {
        return resolveCommonStructType(exp, targetType);
      }

      if (exp.expression() instanceof StringLiteral) {
        final String value = ((StringLiteral) exp.expression()).getValue();

        if (targetType.baseType().isNumber()) {
          return resolveCommonNumericTypeFromStringLiteral(value, targetType);
        }

        if (targetType.baseType() == SqlBaseType.BOOLEAN) {
          validateStringCanBeCoercedToBoolean(value);
          return commonType;
        }
      }

      return resolveCommonSimpleType(exp, targetType);
    }

    private static void validateStringCanBeCoercedToBoolean(final String value) {
      // Attempt to coerce to boolean:
      LITERAL_COERCER.coerce(value, SqlTypes.BOOLEAN)
          .orElseThrow(() -> invalidSyntaxException(value, SqlTypes.BOOLEAN));
    }

    private Optional<SqlType> resolveCommonArrayType(
        final TypedExpression exp,
        final SqlType targetType
    ) {
      final SqlType sourceType = exp.type().orElse(null);
      if (targetType.baseType() != SqlBaseType.ARRAY) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.empty());
      }

      try {
        final SqlArray targetArray = (SqlArray) targetType;
        final CreateArrayExpression sourceArray = (CreateArrayExpression) exp.expression();

        return resolveStructuredCommonType(targetArray.getItemType(), sourceArray.getValues())
            .map(SqlTypes::array);
      } catch (final InvalidCoercionException e) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.of(e));
      }
    }

    private Optional<SqlType> resolveCommonMapType(
        final TypedExpression exp,
        final SqlType targetType
    ) {
      final SqlType sourceType = exp.type().orElse(null);
      if (targetType.baseType() != SqlBaseType.MAP) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.empty());
      }

      try {
        final SqlMap targetMap = (SqlMap) targetType;
        final CreateMapExpression sourceMap = (CreateMapExpression) exp.expression();

        final Optional<SqlType> keyType =
            resolveStructuredCommonType(targetMap.getKeyType(), sourceMap.getMap().keySet());

        final Optional<SqlType> valueType =
            resolveStructuredCommonType(targetMap.getValueType(), sourceMap.getMap().values());

        return keyType.isPresent() && valueType.isPresent()
            ? Optional.of(SqlTypes.map(keyType.get(), valueType.get()))
            : Optional.empty();
      } catch (final InvalidCoercionException e) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.of(e));
      }
    }

    private Optional<SqlType> resolveCommonStructType(
        final TypedExpression exp,
        final SqlType targetType
    ) {
      final SqlType sourceType = exp.type().orElse(null);
      if (targetType.baseType() != SqlBaseType.STRUCT) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.empty());
      }

      try {
        final SqlStruct targetStruct = (SqlStruct) targetType;
        final CreateStructExpression sourceStruct = (CreateStructExpression) exp.expression();

        final List<String> fieldNames = Streams.concat(
            sourceStruct.getFields().stream().map(Field::getName),
            targetStruct.fields().stream().map(SqlStruct.Field::name)
        ).distinct()
            .collect(Collectors.toList());

        final Builder builder = SqlTypes.struct();
        for (final String fieldName : fieldNames) {
          final Optional<Expression> sourceFieldValue = sourceStruct.getFields().stream()
              .filter(f -> f.getName().equals(fieldName))
              .findFirst()
              .map(Field::getValue);

          final Optional<SqlType> sourceFieldType = sourceFieldValue
              .map(sourceExpression ->
                  typeManager.getExpressionSqlType(sourceExpression, lambdaTypeMapping));

          final Optional<SqlType> targetFieldType = targetStruct.field(fieldName)
              .map(SqlStruct.Field::type);

          final SqlType fieldType;
          if (!targetFieldType.isPresent()) {
            fieldType = sourceFieldType.orElseThrow(IllegalStateException::new);
          } else if (!sourceFieldType.isPresent()) {
            fieldType = targetFieldType.orElseThrow(IllegalStateException::new);
          } else {
            fieldType = resolveCommonType(
                new TypedExpression(sourceFieldType.get(), sourceFieldValue.get()),
                targetFieldType
            ).orElseThrow(IllegalStateException::new);
          }

          builder.field(fieldName, fieldType);
        }

        return Optional.of(builder.build());
      } catch (final InvalidCoercionException e) {
        throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.of(e));
      }
    }

    private Optional<SqlType> resolveStructuredCommonType(
        final SqlType targetType,
        final Collection<Expression> expressions
    ) {
      final List<TypedExpression> typedElements = typedExpressions(expressions);
      if (typedElements.isEmpty()) {
        // Either empty or all NULL literals:
        return Optional.of(targetType);
      }

      final List<TypedExpression> typedExpressions = ImmutableList.<TypedExpression>builder()
          .add(new TypedExpression(targetType, new NullLiteral()))
          .addAll(typedElements)
          .build();

      return resolveCommonType(typedExpressions);
    }

    private static Optional<SqlType> resolveCommonNumericTypeFromStringLiteral(
        final String value, 
        final SqlType targetType
    ) {
      Preconditions.checkArgument(targetType.baseType().isNumber());

      try {
        final SqlType sourceType = getStringNumericType(value);

        if (sourceType.baseType() == SqlBaseType.DOUBLE
            || targetType.baseType() == SqlBaseType.DOUBLE
        ) {
          return Optional.of(SqlTypes.DOUBLE);
        }

        if (sourceType.baseType() == SqlBaseType.DECIMAL
            || targetType.baseType() == SqlBaseType.DECIMAL
        ) {
          return Optional.of(DecimalUtil.widen(sourceType, targetType));
        }

        return Optional.of(sourceType.baseType().canImplicitlyCast(targetType.baseType())
            ? targetType
            : sourceType);
      } catch (final NumberFormatException e) {
        throw invalidSyntaxException(value, targetType);
      }
    }

    private static SqlType getStringNumericType(final String value) {
      final BigDecimal result = new BigDecimal(value.trim());

      final boolean containsDpOrScientific = value.contains(".")
          || value.contains("e")
          || value.contains("E");

      if (!containsDpOrScientific && result.scale() == 0) {
        final BigInteger bi = result.toBigIntegerExact();
        if (0 < bi.compareTo(INT_MIN) && bi.compareTo(INT_MAX) < 0) {
          return SqlTypes.INTEGER;
        } else if (0 < bi.compareTo(BIGINT_MIN) && bi.compareTo(BIGINT_MAX) < 0) {
          return SqlTypes.BIGINT;
        }
      }

      return DecimalUtil.fromValue(result);
    }

    private static Optional<SqlType> resolveCommonSimpleType(
        final TypedExpression exp,
        final SqlType targetType
    ) {
      final SqlType sourceType = exp.type().orElseThrow(IllegalStateException::new);

      final DefaultSqlValueCoercer coercer = exp.expression() instanceof Literal
          ? LITERAL_COERCER
          : EXPRESSION_COERCER;

      final Optional<SqlType> common0 = coercer.canCoerce(sourceType, targetType);
      if (common0.isPresent()) {
        return common0;
      }

      final Optional<SqlType> common1 = coercer.canCoerce(targetType, sourceType);
      if (common1.isPresent()) {
        return common1;
      }

      throw coercionFailureException(exp.expression(), sourceType, targetType, Optional.empty());
    }

    private static List<Expression> convertToCommonType(
        final List<TypedExpression> typedExpressions,
        final SqlType commonType
    ) {
      return typedExpressions.stream()
          .map(e -> UserListCoercer.convertToCommonType(e, commonType))
          .collect(Collectors.toList());
    }

    private static Expression convertToCommonType(
        final TypedExpression exp,
        final SqlType commonType
    ) {
      if (exp.type().map(type -> type.equals(commonType)).orElse(true)) {
        // Matching type or Null literal:
        return exp.expression();
      }

      if (exp.expression() instanceof Literal) {
        return convertLiteralToCommonType(exp, commonType);
      }

      return new Cast(exp.expression(), new Type(commonType));
    }

    private static Expression convertLiteralToCommonType(
        final TypedExpression exp,
        final SqlType commonType
    ) {
      final Literal literal = (Literal) exp.expression();

      final Object coerced = LITERAL_COERCER.coerce(literal.getValue(), commonType)
          .orElseThrow(IllegalStateException::new)
          .orElseThrow(IllegalStateException::new);

      return Literals.getFactory(commonType.baseType())
          .apply(coerced);
    }

    private static final class TypedExpression {

      private final Optional<SqlType> type;
      private final Expression expression;

      TypedExpression(final SqlType type, final Expression expression) {
        // Null literals have no type:
        this.type = Optional.ofNullable(type);
        this.expression = requireNonNull(expression, "expression");
      }

      public Optional<SqlType> type() {
        return type;
      }

      public Expression expression() {
        return expression;
      }
    }

    private static KsqlException invalidSyntaxException(
        final Object value,
        final SqlType targetType
    ) {
      return new KsqlException(
          "invalid input syntax for type " + targetType.baseType() + ": \"" + value + "\"."
      );
    }

    private static InvalidCoercionException coercionFailureException(
        final Object value,
        final SqlType sourceType,
        final SqlType targetType,
        final Optional<InvalidCoercionException> cause
    ) {
      final String msg = "operator does not exist: "
          + targetType + " = " + sourceType + " (" + value + ")"
          + System.lineSeparator()
          + "Hint: You might need to add explicit type casts.";

      return cause
          .map(e -> new InvalidCoercionException(msg, e))
          .orElseGet(() -> new InvalidCoercionException(msg));
    }

    private static final class InvalidCoercionException extends KsqlException {

      InvalidCoercionException(final String message) {
        super(message);
      }

      InvalidCoercionException(final String message, final Throwable cause) {
        super(message, cause);
      }
    }
  }
}
