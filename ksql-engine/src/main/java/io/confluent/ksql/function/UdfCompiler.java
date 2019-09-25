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
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdfArgSupplier;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
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

  private static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.<Class<?>>builder()
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
      .add(List.class)
      .add(Map.class)
      .build();

  private static final String UDAF_PACKAGE = "io.confluent.ksql.function.udaf.";

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdfCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
    this.typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
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

  KsqlAggregateFunction<?, ?, ?> compileAggregate(
      final Method method,
      final ClassLoader loader,
      final String functionName,
      final String description,
      final String inputSchema,
      final String aggregateSchema,
      final String outputSchema) {

    final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
                                      method.getName(), functionName, method.getDeclaringClass());

    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
                                  + TableUdaf.class.getName() + ". "
                                  + functionInfo);
    }

    final UdafTypes types = new UdafTypes(method, functionName);
    final Schema inputValue = types.getInputSchema(inputSchema);
    final List<Schema> args = Collections.singletonList(inputValue);
    final Schema aggregateValue = types.getAggregateSchema(aggregateSchema);
    final Schema returnValue = types.getOutputSchema(outputSchema);

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
                  + "(args, aggregateType, outputType, metrics);",
              UdfArgSupplier.class, new String[]{"args", "aggregateType", "outputType", "metrics"});

      return evaluator.apply(args, aggregateValue, returnValue, metrics);
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile KSqlAggregateFunction for method='"
          + method.getName() + "' in class='" + method.getDeclaringClass() + "'", e);
    }
  }

  private class UdafTypes {

    private final Type inputType;
    private final Type aggregateType;
    private final Type outputType;
    private final Method method;
    private final String functionInfo;
    private final String invalidClassErrorMsg;

    UdafTypes(final Method m, final String functionInfo) {
      this.method = Objects.requireNonNull(m);
      this.functionInfo = Objects.requireNonNull(functionInfo);
      this.invalidClassErrorMsg = "class='%s'"
          + " is not supported by UDAFs. Valid types are: " + SUPPORTED_TYPES + " "
          + functionInfo;
      final AnnotatedParameterizedType annotatedReturnType
          = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
      final ParameterizedType type = (ParameterizedType) annotatedReturnType.getType();

      inputType = type.getActualTypeArguments()[0];
      aggregateType = type.getActualTypeArguments()[1];
      outputType = type.getActualTypeArguments()[2];

      validateTypes(inputType);
      validateTypes(aggregateType);
      validateTypes(outputType);
    }

    private void validateTypes(final Type t) {
      if (isUnsupportedType((Class<?>) getRawType(t))) {
        throw new KsqlException(String.format(invalidClassErrorMsg, t));
      }
    }

    Schema getInputSchema(final String inSchema) {
      validateStructAnnotation(inputType, inSchema, "paramSchema");
      final Schema inputSchema = getSchemaFromType(inputType, inSchema);
      //Currently, aggregate functions cannot have reified types as input parameters.
      if (!GenericsUtil.constituentGenerics(inputSchema).isEmpty()) {
        throw new KsqlException("Generic type parameters containing reified types are not currently"
                                    + " supported. " + functionInfo);
      }
      return inputSchema;
    }

    Schema getAggregateSchema(final String aggSchema) {
      validateStructAnnotation(aggregateType, aggSchema, "aggregateSchema");
      return getSchemaFromType(aggregateType, aggSchema);
    }

    Schema getOutputSchema(final String outSchema) {
      validateStructAnnotation(outputType, outSchema, "returnSchema");
      return getSchemaFromType(outputType, outSchema);
    }

    private void validateStructAnnotation(final Type type, final String schema, final String msg) {
      if (type.equals(Struct.class) && schema.isEmpty()) {
        throw new KsqlException("Must specify '" + msg + "' for STRUCT parameter in @UdafFactory.");
      }
    }

    private Schema getSchemaFromType(final Type type, final String schema) {
      return schema.isEmpty()
          ? SchemaUtil.ensureOptional(UdfUtil.getSchemaFromType(type))
          : SchemaConverters.sqlToConnectConverter()
              .toConnectSchema(typeParser.parse(schema).getSqlType());
    }

    private Type getRawType(final Type type) {
      if (type instanceof ParameterizedType) {
        return ((ParameterizedType) type).getRawType();
      }
      return type;
    }
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
        .filter(UdfCompiler::isUnsupportedType)
        .findFirst()
        .ifPresent(type -> {
          throw new KsqlException(
              String.format(
                  "Type %s is not supported by UDAF Factory methods. "
                      + "Supported types %s. functionName=%s, method=%s, class=%s",
                  type,
                  SUPPORTED_TYPES,
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
    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      final Class<?> type = types[i];

      if (method.getGenericParameterTypes()[i] instanceof TypeVariable
          || method.getGenericParameterTypes()[i] instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (isUnsupportedType(type)) {
        throw new KsqlException(
            String.format(
                "Type %s is not supported by UDF methods. "
                    + "Supported types %s. method=%s, class=%s",
                type,
                SUPPORTED_TYPES,
                method.getName(),
                method.getDeclaringClass()
            )
        );
      }
    }

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

  private static boolean isUnsupportedType(final Class<?> type) {
    return !SUPPORTED_TYPES.contains(type)
        && (!type.isArray() || !SUPPORTED_TYPES.contains(type.getComponentType()))
        && SUPPORTED_TYPES.stream().noneMatch(supported -> supported.isAssignableFrom(type));
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
