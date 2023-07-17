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

package io.confluent.ksql.execution.interpreter;

import io.confluent.ksql.execution.codegen.helpers.CastEvaluator;
import io.confluent.ksql.execution.interpreter.terms.CastTerm;
import io.confluent.ksql.execution.interpreter.terms.CastTerm.CastFunction;
import io.confluent.ksql.execution.interpreter.terms.CastTerm.ComparableCastFunction;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.schema.ksql.SqlBooleans;
import io.confluent.ksql.schema.ksql.SqlDoubles;
import io.confluent.ksql.schema.ksql.SqlTimestamps;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class CastInterpreter {
  private CastInterpreter() { }

  public static CastTerm cast(
      final Term term,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    return new CastTerm(term, to, castFunction(from, to, config));
  }

  private static CastFunction castFunction(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlBaseType toBaseType = to.baseType();
    if (toBaseType == SqlBaseType.INTEGER) {
      return castToIntegerFunction(from);
    } else if (toBaseType == SqlBaseType.BIGINT) {
      return castToLongFunction(from);
    } else if (toBaseType == SqlBaseType.DOUBLE) {
      return castToDoubleFunction(from);
    } else if (toBaseType == SqlBaseType.DECIMAL) {
      return castToBigDecimalFunction(from, to);
    } else if (toBaseType == SqlBaseType.STRING) {
      return castToStringFunction(from, config);
    } else if (toBaseType == SqlBaseType.BOOLEAN) {
      return castToBooleanFunction(from);
    } else if (toBaseType == SqlBaseType.TIMESTAMP) {
      return castToTimestampFunction(from);
    } else if (toBaseType == SqlBaseType.ARRAY) {
      return castToArrayFunction(from, to, config);
    } else if (toBaseType == SqlBaseType.MAP) {
      return castToMapFunction(from, to, config);
    } else {
      throw new KsqlException("Unsupported cast from " + from + " to " + to);
    }
  }

  public static ComparableCastFunction<Integer> castToIntegerFunction(final SqlType from) {
    switch (from.baseType()) {
      case STRING:
        return object -> Integer.parseInt(((String) object).trim());
      case DECIMAL:
        return object -> ((BigDecimal) object).intValue();
      case DOUBLE:
        return object -> ((Double) object).intValue();
      case INTEGER:
        return object -> ((Integer) object);
      case BIGINT:
        return object -> ((Long) object).intValue();
      default:
        throw new KsqlException(getErrorMessage(from, SqlTypes.INTEGER));
    }
  }

  public static ComparableCastFunction<Double> castToDoubleFunction(final SqlType from) {
    switch (from.baseType()) {
      case STRING:
        return object -> SqlDoubles.parseDouble(((String) object).trim());
      case DECIMAL:
        return object -> ((BigDecimal) object).doubleValue();
      case DOUBLE:
        return object -> (Double) object;
      case INTEGER:
        return object -> ((Integer) object).doubleValue();
      case BIGINT:
        return object -> ((Long) object).doubleValue();
      default:
        throw new KsqlException(getErrorMessage(from, SqlTypes.DOUBLE));
    }
  }

  public static ComparableCastFunction<Long> castToLongFunction(final SqlType from) {
    switch (from.baseType()) {
      case STRING:
        return object -> Long.parseLong(((String) object).trim());
      case DECIMAL:
        return object -> ((BigDecimal) object).longValue();
      case DOUBLE:
        return object -> ((Double) object).longValue();
      case INTEGER:
        return object -> ((Integer) object).longValue();
      case BIGINT:
        return object -> ((Long) object);
      default:
        throw new KsqlException(getErrorMessage(from, SqlTypes.BIGINT));
    }
  }

  public static CastFunction castToBigDecimalFunction(final SqlType from, final SqlType to) {
    final SqlDecimal sqlDecimal = (SqlDecimal) to;
    switch (from.baseType()) {
      case INTEGER:
        return object -> DecimalUtil.cast(((Integer) object),
            sqlDecimal.getPrecision(), sqlDecimal.getScale());
      case BIGINT:
        return object -> DecimalUtil.cast(((Long) object), sqlDecimal.getPrecision(),
            sqlDecimal.getScale());
      case DOUBLE:
        return object -> DecimalUtil.cast(((Double) object), sqlDecimal.getPrecision(),
            sqlDecimal.getScale());
      case DECIMAL:
        return object -> DecimalUtil.cast(((BigDecimal) object), sqlDecimal.getPrecision(),
            sqlDecimal.getScale());
      case STRING:
        return object -> DecimalUtil.cast(((String) object), sqlDecimal.getPrecision(),
            sqlDecimal.getScale());
      default:
        throw new KsqlException(String.format("Unsupported cast between %s and %s", from,
            SqlBaseType.DECIMAL));
    }
  }

  public static ComparableCastFunction<BigDecimal> castToBigDecimalFunction(final SqlType from) {
    switch (from.baseType()) {
      case DECIMAL:
        return object -> (BigDecimal) object;
      case DOUBLE:
        return object -> BigDecimal.valueOf((Double) object);
      case INTEGER:
        return object -> new BigDecimal((Integer) object);
      case BIGINT:
        return object -> new BigDecimal((Long) object);
      case STRING:
        return object -> new BigDecimal((String) object);
      default:
        throw new KsqlException(String.format("Unsupported cast between %s and %s", from,
            SqlBaseType.DECIMAL));
    }
  }

  public static CastFunction castToStringFunction(
      final SqlType from,
      final KsqlConfig config
  ) {
    if (from.baseType() == SqlBaseType.DECIMAL) {
      return object -> ((BigDecimal) object).toPlainString();
    } else if (from.baseType() == SqlBaseType.TIMESTAMP) {
      return object -> SqlTimestamps.formatTimestamp(((Timestamp) object));
    }
    return object -> config.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)
        ? Objects.toString(object, null)
        : String.valueOf(object);
  }

  public static CastFunction castToBooleanFunction(
      final SqlType from
  ) {
    if (from.baseType() == SqlBaseType.STRING) {
      return object -> SqlBooleans.parseBoolean(((String) object).trim());
    } else if (from.baseType() == SqlBaseType.BOOLEAN) {
      return object -> object;
    }
    throw new KsqlException("Unsupported cast to BOOLEAN: " + from);
  }

  public static ComparableCastFunction<Date> castToTimestampFunction(
      final SqlType from
  ) {
    if (from.baseType() == SqlBaseType.STRING) {
      return object -> SqlTimestamps.parseTimestamp(((String) object).trim());
    } else if (from.baseType() == SqlBaseType.TIMESTAMP) {
      return object -> (Timestamp) object;
    }
    throw new KsqlException("Unsupported cast to TIMESTAMP: " + from);
  }

  public static CastFunction castToArrayFunction(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    if (from.baseType() == SqlBaseType.ARRAY
        && to.baseType() == SqlBaseType.ARRAY) {
      final SqlArray fromArray = (SqlArray) from;
      final SqlArray toArray = (SqlArray) to;
      final CastFunction itemCastFunction = castFunction(
          fromArray.getItemType(), toArray.getItemType(), config);
      return o ->
          CastEvaluator.castArray((List<?>) o, itemCastFunction::cast);
    }
    throw new KsqlException(getErrorMessage(from, to));
  }

  public static CastFunction castToMapFunction(
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    if (from.baseType() == SqlBaseType.MAP
        && to.baseType() == SqlBaseType.MAP) {
      final SqlMap fromMap = (SqlMap) from;
      final SqlMap toMap = (SqlMap) to;
      final CastFunction keyCastFunction = castFunction(
          fromMap.getKeyType(), toMap.getKeyType(), config);
      final CastFunction valueCastFunction = castFunction(
          fromMap.getValueType(), toMap.getValueType(), config);
      return o ->
          CastEvaluator.castMap((Map<?, ?>) o, keyCastFunction::cast, valueCastFunction::cast);
    }
    throw new KsqlException("Unsupported cast to " + to + ": " + from);
  }

  private static String getErrorMessage(
      final SqlType from,
      final SqlType to
  ) {
    return String.format("Unsupported cast from %s to %s", from, to);
  }
}
