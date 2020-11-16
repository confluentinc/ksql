package io.confluent.ksql.execution.codegen;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
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
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  public static Object cookAndEval(
      final String javaCode,
      final Class<?> resultType,
      final String argName,
      final Class<?> argType,
      final Object arg
  ) {
    return cookAndEval(
      javaCode,
      resultType,
        ImmutableList.of(argName),
        ImmutableList.of(argType),
        Collections.singletonList(arg)
    );
  }

  public static Object cookAndEval(
      final String javaCode,
      final Class<?> resultType,
      final List<String> argNames,
      final List<Class<?>> argTypes,
      final List<Object> args
  ) {
    final Evaluator evaluator = CodeGenTestUtil.cookCode(javaCode, resultType, argNames, argTypes);
    return evaluator.evaluate(args);
  }

  public static Evaluator cookCode(
      final String javaCode,
      final Class<?> resultType
  ) {
    return cookCode(
        javaCode,
        resultType,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  public static Evaluator cookCode(
      final String javaCode,
      final Class<?> resultType,
      final String argName,
      final Class<?> argType
  ) {
    return cookCode(
        javaCode,
        resultType,
        ImmutableList.of(argName),
        ImmutableList.of(argType)
    );
  }

  public static Evaluator cookCode(
      final String javaCode,
      final Class<?> resultType,
      final List<String> argNames,
      final List<Class<?>> argTypes
  ) {
    try {
      final IExpressionEvaluator ee = CodeGenRunner.cook(
          javaCode,
          resultType,
          argNames.toArray(new String[0]),
          argTypes.toArray(new Class<?>[0])
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

    public Object evaluate(final Object arg) {
      return evaluate(Collections.singletonList(arg));
    }

    public Object evaluate(final List<?> args) {
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

    public Object rawEvaluate(final Object arg) throws Exception {
      return rawEvaluate(Collections.singletonList(arg));
    }

    public Object rawEvaluate(final List<?> args) throws Exception {
      try {
        return ee.evaluate(args == null ? new Object[]{null} : args.toArray());
      } catch (final InvocationTargetException e) {
        throw e.getTargetException() instanceof Exception
            ? (Exception) e.getTargetException()
            : e;
      }
    }
  }
}
