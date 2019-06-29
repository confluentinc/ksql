/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdfArgSupplier;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.TypeContextUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.resource.Resource;
import org.codehaus.janino.util.resource.ResourceFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes methods that have been marked with the Udf or UdfFactory annotation.
 * Each method gets a class generated for it. For Udfs it is an {@link UdfInvoker}.
 * For UDAFs it is a {@link KsqlAggregateFunction}
 */
@Immutable
public class UdfCompiler {
  private static final Logger LOGGER = LoggerFactory.getLogger(UdfCompiler.class);

  private static final Set<Class<?>> SUPPORTED_UDAF_TYPES = ImmutableSet.<Class<?>>builder()
      .add(int.class)
      .add(long.class)
      .add(double.class)
      .add(boolean.class)
      .add(Integer.class)
      .add(Long.class)
      .add(Double.class)
      .add(BigDecimal.class)
      .add(Boolean.class)
      .add(String.class)
      .add(Struct.class)
      .build();

  private static final Set<Class<?>> SUPPORTED_UDF_TYPES = ImmutableSet.<Class<?>>builder()
      .addAll(SUPPORTED_UDAF_TYPES)
      .add(List.class)
      .add(Map.class)
      .build();

  private static final String UDAF_PACKAGE = "io.confluent.ksql.function.udaf.";

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UdfCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
  }

  @VisibleForTesting
  public static UdfInvoker compile(final Method method, final ClassLoader loader) {
    try {
      final IScriptEvaluator scriptEvaluator = createScriptEvaluator(method,
          loader,
          method.getDeclaringClass().getName());
      final String code = generateCode(method);
      return (UdfInvoker) scriptEvaluator.createFastEvaluator(code,
          UdfInvoker.class, new String[]{"thiz", "args"});
    } catch (final KsqlException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile udf wrapper class for "
          + method, e);
    }
  }

  KsqlAggregateFunction<?, ?> compileAggregate(
      final Method method,
      final ClassLoader loader,
      final String functionName,
      final String description,
      final String paramSchema,
      final String returnSchema
  ) {
    final Pair<Type, Type> valueAndAggregateTypes
        = getValueAndAggregateTypes(method, functionName);
    if (valueAndAggregateTypes.left.equals(Struct.class) && paramSchema.isEmpty()) {
      throw new KsqlException("Must specify 'paramSchema' for STRUCT parameter in @UdafFactory.");
    }
    if (valueAndAggregateTypes.right.equals(Struct.class) && returnSchema.isEmpty()) {
      throw new KsqlException("Must specify 'returnSchema' to return STRUCT from @UdafFactory.");
    }

    try {
      final String generatedClassName
          = method.getDeclaringClass().getSimpleName() + "_" + method.getName() + "_Aggregate";
      final String udafClass = generateUdafClass(generatedClassName,
          method,
          functionName,
          description);
      LOGGER.trace("Generated class for functionName={}, method={} class\n{}\n",
          functionName,
          method.getName(),
          udafClass);
      final ClassLoader javaSourceClassLoader
          = createJavaSourceClassLoader(loader, generatedClassName, udafClass);

      final IScriptEvaluator scriptEvaluator =
          createScriptEvaluator(method,
              javaSourceClassLoader,
              UDAF_PACKAGE + generatedClassName);
      final UdfArgSupplier evaluator = (UdfArgSupplier)
          scriptEvaluator.createFastEvaluator("return new " + generatedClassName
                  + "(args, returnType, metrics);",
              UdfArgSupplier.class, new String[]{"args", "returnType", "metrics"});

      final Schema argSchema = paramSchema.isEmpty()
          ? SchemaUtil.getSchemaFromType(valueAndAggregateTypes.left)
          : SchemaConverters.sqlToLogicalConverter()
              .fromSqlType(TypeContextUtil.getType(paramSchema).getSqlType());
      final List<Schema> args = Collections.singletonList(argSchema);

      final Schema returnValue = returnSchema.isEmpty()
          ? SchemaUtil.ensureOptional(SchemaUtil.getSchemaFromType(valueAndAggregateTypes.right))
          : SchemaConverters.sqlToLogicalConverter()
              .fromSqlType(TypeContextUtil.getType(returnSchema).getSqlType());

      return evaluator.apply(args, returnValue, metrics);
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile KSqlAggregateFunction for method='"
          + method.getName() + "' in class='" + method.getDeclaringClass() + "'", e);
    }
  }

  private static Pair<Type, Type> getValueAndAggregateTypes(
      final Method method,
      final String functionName
  ) {
    final String functionInfo = "method='" + method.getName()
        + "', functionName='" + functionName + "' UDFClass='" + method.getDeclaringClass() + "'";
    final String invalidClass = "class='%s'"
        + " is not supported by UDAFs. Valid types are: " + SUPPORTED_UDAF_TYPES + " "
        + functionInfo;

    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
          + TableUdaf.class.getName() + " ."
          + functionInfo);
    }
    final AnnotatedParameterizedType annotatedReturnType
        = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
    final ParameterizedType type = (ParameterizedType) annotatedReturnType.getType();
    final Type valueType = type.getActualTypeArguments()[0];
    final Type aggregateType = type.getActualTypeArguments()[1];
    if (!isTypeSupported((Class<?>) getRawType(aggregateType), SUPPORTED_UDF_TYPES)) {
      throw new KsqlException(String.format(invalidClass, aggregateType));
    }
    if (!isTypeSupported((Class<?>) getRawType(valueType), SUPPORTED_UDF_TYPES)) {
      throw new KsqlException(String.format(invalidClass, valueType));
    }
    return new Pair<>(valueType, aggregateType);
  }

  private static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  private static JavaSourceClassLoader createJavaSourceClassLoader(
      final ClassLoader loader,
      final String generatedClassName,
      final String udafClass
  ) {
    final long lastMod = System.currentTimeMillis();
    return new JavaSourceClassLoader(loader, new ResourceFinder() {
      @Override
      public Resource findResource(final String resource) {
        if (resource.endsWith(generatedClassName + ".java")) {
          return new Resource() {
            @Override
            public InputStream open() throws IOException {
              return new ByteArrayInputStream(udafClass.getBytes(StandardCharsets.UTF_8.name()));
            }

            @Override
            public String getFileName() {
              return resource;
            }

            @Override
            public long lastModified() {
              return lastMod;
            }
          };
        }
        return null;
      }
    }, StandardCharsets.UTF_8.name());
  }

  private static String generateUdafClass(
      final String generatedClassName,
      final Method method,
      final String functionName,
      final String description
  ) {
    validateMethodSignature(method);
    Arrays.stream(method.getParameterTypes())
        .filter(type -> !UdfCompiler.isTypeSupported(type, SUPPORTED_UDAF_TYPES))
        .findFirst()
        .ifPresent(type -> {
          throw new KsqlException(
              String.format(
                  "Type %s is not supported by UDAF Factory methods. "
                      + "Supported types %s. functionName=%s, method=%s, class=%s",
                  type,
                  SUPPORTED_UDAF_TYPES,
                  functionName,
                  method.getName(),
                  method.getDeclaringClass()
              )
          );
        });

    return UdafTemplate.generateCode(method, generatedClassName, functionName, description);
  }

  /**
   * Generates code for the given method.
   * @param method  the UDF to generate the code for
   * @return String representation of the code that should be compiled for the UDF
   */
  private static String generateCode(final Method method) {
    validateMethodSignature(method);
    Arrays.stream(method.getParameterTypes())
        .filter(type -> !UdfCompiler.isTypeSupported(type, SUPPORTED_UDF_TYPES))
        .findFirst()
        .ifPresent(type -> {
          throw new KsqlException(
              String.format(
                  "Type %s is not supported by UDF methods. "
                      + "Supported types %s. method=%s, class=%s",
                  type,
                  SUPPORTED_UDAF_TYPES,
                  method.getName(),
                  method.getDeclaringClass()
              )
          );
        });

    return UdfTemplate.generateCode(method, "thiz");
  }

  private static void validateMethodSignature(final Method method) {
    for (int i = 0; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i].isArray()) {
        if (!method.isVarArgs() || i != method.getParameterCount() - 1) {
          throw new KsqlFunctionException(
              "Invalid UDF method signature (contains non var-arg array): " + method);
        }
      }
    }
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private static boolean isTypeSupported(final Class<?> type, final Set<Class<?>> supportedTypes) {
    return supportedTypes.contains(type)
        || type.isArray() && supportedTypes.contains(type.getComponentType())
        || supportedTypes.stream().anyMatch(supported -> supported.isAssignableFrom(type));
  }

  private static IScriptEvaluator createScriptEvaluator(
      final Method method,
      final ClassLoader loader,
      final String udfClass
  ) throws Exception {
    final IScriptEvaluator scriptEvaluator
        = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    scriptEvaluator.setClassName(method.getDeclaringClass().getName() + "_" + method.getName());
    scriptEvaluator.setDefaultImports(new String[]{
        "java.util.*",
        "io.confluent.ksql.function.KsqlFunctionException",
        udfClass,
    });
    scriptEvaluator.setParentClassLoader(loader);
    return scriptEvaluator;
  }
}
