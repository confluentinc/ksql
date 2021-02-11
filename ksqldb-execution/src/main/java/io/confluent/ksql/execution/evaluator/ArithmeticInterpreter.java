package io.confluent.ksql.execution.evaluator;

import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.util.KsqlException;

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
}
