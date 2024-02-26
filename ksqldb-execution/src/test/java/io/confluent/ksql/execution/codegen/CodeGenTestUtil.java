package io.confluent.ksql.execution.codegen;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public final class CodeGenTestUtil {

  private CodeGenTestUtil() {}

  public static Object cookAndEval(
      final String javaCode,
      final Class<?> resultType
  ) {
    return cookAndEval(
        javaCode,
        resultType,
        Collections.emptyMap()
    );
  }

  public static Object cookAndEval(
      final String javaCode,
      final Class<?> resultType,
      final Map<String, Object> args
  ) {
    final Evaluator evaluator = CodeGenTestUtil.cookCode(javaCode, resultType);
    return evaluator.evaluate(args);
  }

  public static Evaluator cookCode(
      final String javaCode,
      final Class<?> resultType
  ) {
    try {
      final IExpressionEvaluator ee = CodeGenRunner.cook(
          javaCode,
          resultType
      );

      return new Evaluator(ee, javaCode);
    } catch (final Exception e) {
      throw new AssertionError(
          "Failed to compile generated code"
              + System.lineSeparator()
              + javaCode,
          e
      );
    }
  }

  public static final class Evaluator {

    final IExpressionEvaluator ee;
    final String javaCode;

    public Evaluator(final IExpressionEvaluator ee, final String javaCode) {
      this.ee = requireNonNull(ee, "ee");
      this.javaCode = requireNonNull(javaCode, "javaCode");
    }

    public Object evaluate() {
      return evaluate(Collections.emptyMap());
    }

    public Object evaluate(final String argName, final Object argValue) {
      return evaluate(Collections.singletonMap(argName, argValue));
    }

    public Object evaluate(final Map<String, Object> args) {
      try {
        return rawEvaluate(args);
      } catch (final Exception e) {
        throw new AssertionError(
            "Failed to eval generated code"
                + System.lineSeparator()
                + javaCode,
            e
        );
      }
    }

    public Object rawEvaluate(final String argName, final Object argValue) throws Exception {
      return rawEvaluate(Collections.singletonMap(argName, argValue));
    }

    public Object rawEvaluate(final Map<String, Object> args) throws Exception {
      try {
        return ee.evaluate(new Object[]{args, null, null, null});
      } catch (final InvocationTargetException e) {
        throw e.getTargetException() instanceof Exception
            ? (Exception) e.getTargetException()
            : e;
      }
    }
  }
}
