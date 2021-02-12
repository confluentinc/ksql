package io.confluent.ksql.execution.evaluator;

import io.confluent.ksql.execution.codegen.helpers.CastEvaluator;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CastInterpreter {

  public static Object cast(
      final Object object,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    final SqlBaseType toBaseType = to.baseType();
    if (toBaseType == SqlBaseType.INTEGER) {
      return castToInteger(object, from);
    } else if (toBaseType == SqlBaseType.BIGINT) {
      return castToLong(object, from);
    } else if (toBaseType == SqlBaseType.DOUBLE) {
      return castToDouble(object, from);
    } else if (toBaseType == SqlBaseType.DECIMAL) {
      return castToBigDecimal(object, from, to);
    } else if (toBaseType == SqlBaseType.STRING) {
      return castToString(object, from, config);
    } else if (toBaseType == SqlBaseType.BOOLEAN) {
      return castToBoolean(object, from);
    } else if (toBaseType == SqlBaseType.ARRAY) {
      return castToArray(object, from, to, config);
    } else if (toBaseType == SqlBaseType.MAP) {
      return castToMap(object, from, to, config);
    } else {
      throw new KsqlException("Unsupported cast from " + from + " to " + to);
    }
  }

  public static Integer castToInteger(
      final Object object,
      final SqlType from
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.STRING) {
      return Integer.parseInt(((String) object).trim());
    }
    return NumberConversions.toInteger(object, from, ConversionType.CAST);
  }

  public static Double castToDouble(
      final Object object,
      final SqlType from
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.STRING) {
      return SqlDoubles.parseDouble(((String) object).trim());
    }
    return NumberConversions.toDouble(object, from, ConversionType.CAST);
  }

  public static Long castToLong(
      final Object object,
      final SqlType from
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.STRING) {
      return Long.parseLong(((String) object).trim());
    }
    return NumberConversions.toLong(object, from, ConversionType.CAST);
  }

  public static BigDecimal castToBigDecimal(
      final Object object,
      final SqlType from,
      final SqlType to
  ) {
    if (object == null) {
      return null;
    }
    final SqlDecimal sqlDecimal = (SqlDecimal) to;
    if (from.baseType() == SqlBaseType.INTEGER) {
      return DecimalUtil.cast(((Integer) object), sqlDecimal.getPrecision(), sqlDecimal.getScale());
    } else if (from.baseType() == SqlBaseType.BIGINT) {
      return DecimalUtil.cast(((Long) object), sqlDecimal.getPrecision(), sqlDecimal.getScale());
    } else if (from.baseType() == SqlBaseType.DOUBLE) {
      return DecimalUtil.cast(((Double) object), sqlDecimal.getPrecision(), sqlDecimal.getScale());
    } else if (from.baseType() == SqlBaseType.DECIMAL) {
      return DecimalUtil.cast(((BigDecimal) object), sqlDecimal.getPrecision(),
          sqlDecimal.getScale());
    } else if (from.baseType() == SqlBaseType.STRING) {
      return DecimalUtil.cast(((String) object), sqlDecimal.getPrecision(), sqlDecimal.getScale());
    }
    throw new KsqlException("Unsupported type cast to BigDecimal: " + from);
  }

  public static String castToString(
      final Object object,
      final SqlType from,
      final KsqlConfig config
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.DECIMAL) {
      return ((BigDecimal) object).toPlainString();
    } else if (from.baseType() == SqlBaseType.TIMESTAMP) {
      return SqlTimestamps.formatTimestamp(((Timestamp) object));
    }
    return config.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE)
        ? Objects.toString(object, null)
        : String.valueOf(object);
  }

  public static Boolean castToBoolean(
      final Object object,
      final SqlType from
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.STRING) {
      return SqlBooleans.parseBoolean(((String) object).trim());
    }
    throw new KsqlException("Unsupported cast to BOOLEAN: " + from);
  }

  public static List<Object> castToArray(
      final Object object,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.ARRAY &&
        to.baseType() == SqlBaseType.ARRAY) {
      final SqlArray fromArray = (SqlArray) from;
      final SqlArray toArray = (SqlArray) to;
      return CastEvaluator.castArray((List<?>)object,
          o -> cast(o, fromArray.getItemType(), toArray.getItemType(), config));
    }
    throw new KsqlException(getErrorMessage(ConversionType.CAST, from, to));
  }

  public static Map<Object, Object> castToMap(
      final Object object,
      final SqlType from,
      final SqlType to,
      final KsqlConfig config
  ) {
    if (object == null) {
      return null;
    }
    if (from.baseType() == SqlBaseType.MAP &&
        to.baseType() == SqlBaseType.MAP) {
      final SqlMap fromMap = (SqlMap) from;
      final SqlMap toMap = (SqlMap) to;
      return CastEvaluator.castMap((Map<?, ?>)object,
          k -> cast(k, fromMap.getKeyType(), toMap.getKeyType(), config),
          v -> cast(v, fromMap.getValueType(), toMap.getValueType(), config));
    }
    throw new KsqlException("Unsupported cast to " + to + ": " + from);
  }

  /**
   * Conversion functions that work for converting type either during comparison or casting.
   */
  public static class NumberConversions {
    public static Double toDouble(final Object object, final SqlType from,
        final ConversionType type) {
      if (object == null) {
        return null;
      }
      if (object instanceof Number) {
        return ((Number) object).doubleValue();
      } else {
        throw new KsqlException(getErrorMessage(type, from, SqlTypes.DOUBLE));
      }
    }

    public static Long toLong(final Object object, final SqlType from, final ConversionType type) {
      if (object == null) {
        return null;
      }
      if (object instanceof Number) {
        return ((Number) object).longValue();
      } else {
        throw new KsqlException(getErrorMessage(type, from, SqlTypes.BIGINT));
      }
    }

    public static Integer toInteger(final Object object, final SqlType from,
        final ConversionType type) {
      if (object == null) {
        return null;
      }
      if (object instanceof Number) {
        return ((Number) object).intValue();
      } else {
        throw new KsqlException(getErrorMessage(type, from, SqlTypes.INTEGER));
      }
    }
  }

  private static String getErrorMessage(
      final ConversionType type,
      final SqlType from,
      final SqlType to
  ) {
    switch (type) {
      case CAST:
        return String.format("Unsupported cast from %s to %s", from, to);
      case COMPARISON:
        return String.format("Unsupported comparison between %s and %s", from, to);
      case ARITHMETIC:
        return String.format("Unsupported arithmetic between %s and %s", from, to);
      default:
        throw new KsqlException("Unknown Conversion type " + type);
    }
  }

  enum ConversionType {
    CAST,
    COMPARISON,
    ARITHMETIC
  }
}
