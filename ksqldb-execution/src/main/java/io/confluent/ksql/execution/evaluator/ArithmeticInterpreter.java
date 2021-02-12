package io.confluent.ksql.execution.evaluator;

import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class ArithmeticInterpreter {
  public static Double apply(final Operator operator, final Double a, final Double b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  public static Integer apply(final Operator operator, final Integer a, final Integer b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  public static Long apply(final Operator operator, final Long a, final Long b) {
    switch (operator) {
      case ADD:
        return a + b;
      case SUBTRACT:
        return a - b;
      case MULTIPLY:
        return a * b;
      case DIVIDE:
        return a / b;
      case MODULUS:
        return a % b;
      default:
        throw new KsqlException("Unknown operator " + operator);
    }
  }

  public static BigDecimal apply(
      final SqlDecimal decimal,
      final Operator operator,
      final BigDecimal a,
      final BigDecimal b) {
    final MathContext mc = new MathContext(decimal.getPrecision(),
        RoundingMode.UNNECESSARY);
    switch (operator) {
      case ADD:
        return a.add(b, mc).setScale(decimal.getScale());
      case SUBTRACT:
        return a.subtract(b, mc).setScale(decimal.getScale());
      case MULTIPLY:
        return a.multiply(b, mc).setScale(decimal.getScale());
      case DIVIDE:
        return a.divide(b, mc).setScale(decimal.getScale());
      case MODULUS:
        return a.remainder(b, mc).setScale(decimal.getScale());
      default:
        throw new KsqlException("DECIMAL operator not supported: " + operator);
    }
  }
}
