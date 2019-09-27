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
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udaf.UdfArgSupplier;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
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
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
  private static final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdfCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
  }

  @VisibleForTesting
  public static UdfInvoker compile(final Method method, final ClassLoader loader) {
    try {
      validateUdfMethodSignature(method);
      validateUdfInputParameters(method);
      final IScriptEvaluator scriptEvaluator = createScriptEvaluator(method,
          loader,
          method.getDeclaringClass().getName());
      final String code = UdfTemplate.generateCode(method, "thiz");
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
      final String description
  ) {

    final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
                                      method.getName(), functionName, method.getDeclaringClass());

    validateUdafMethodSignature(method, functionName);
    validateUdafInputParameters(method);

    final UdafTypes types = new UdafTypes(method, functionName);
    final Schema inputValue = types.getInputSchema();
    final List<Schema> args = Collections.singletonList(inputValue);
    final Schema aggregateValue = types.getAggregateSchema();
    final Schema returnValue = types.getOutputSchema();

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
    private final Optional<UdafFactory> annotation;

    UdafTypes(final Method m, final String functionInfo) {
      this.method = Objects.requireNonNull(m);
      final AnnotatedParameterizedType annotatedReturnType
          = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
      final ParameterizedType type = (ParameterizedType) annotatedReturnType.getType();
      this.annotation = method.getAnnotation(UdafFactory.class) == null
          ? Optional.empty() : Optional.of(method.getAnnotation(UdafFactory.class));

      inputType = type.getActualTypeArguments()[0];
      aggregateType = type.getActualTypeArguments()[1];
      outputType = type.getActualTypeArguments()[2];
    }

    Schema getInputSchema() {
      final Optional<String> inSchema = annotation.map(UdafFactory::paramSchema).filter(
          val -> !val.isEmpty());
      return parseParameter(inputType, inSchema, "paramSchema", "", "");
    }

    Schema getAggregateSchema() {
      final Optional<String> aggSchema = annotation.map(UdafFactory::aggregateSchema).filter(
          val -> !val.isEmpty());
      return parseParameter(aggregateType, aggSchema, "aggregateSchema", "", "");
    }

    Schema getOutputSchema() {
      final Optional<String> outSchema = annotation.map(UdafFactory::returnSchema).filter(
          val -> !val.isEmpty());
      return parseParameter(outputType, outSchema, "returnSchema", "", "");
    }
  }

  static List<Schema> parseUdfInputParameters(final Method method,
                                              final UdfDescription classLevelAnnotation) {

    final List<Schema> inputSchemas = new ArrayList<>(method.getParameterCount());
    for (int idx = 0; idx < method.getParameterCount(); idx++) {

      final Type type = method.getGenericParameterTypes()[idx];
      final Optional<UdfParameter> annotation = Arrays.stream(method.getParameterAnnotations()[idx])
          .filter(UdfParameter.class::isInstance)
          .map(UdfParameter.class::cast)
          .findAny();

      final Parameter param = method.getParameters()[idx];
      final String name = annotation.map(UdfParameter::value)
          .filter(val -> !val.isEmpty())
          .orElse(param.isNamePresent() ? param.getName() : null);

      if (name == null) {
        throw new KsqlFunctionException(
            String.format("Cannot resolve parameter name for param at index %d for UDF %s:%s. "
                              + "Please specify a name in @UdfParameter or compile your JAR with"
                              + " -parameters to infer the name from the parameter name.",
                          idx, classLevelAnnotation.name(), method.getName()));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      final Optional<String> schemaString = annotation.isPresent()
          && !annotation.get().schema().isEmpty()
          ? Optional.of(annotation.get().schema()) : Optional.empty();

      inputSchemas.add(parseParameter(type, schemaString, name, name, doc));
    }
    return inputSchemas;
  }

  private static Schema parseParameter(final Type type,
                                       final Optional<String> schemaString,
                                       final String msg,
                                       final String name,
                                       final String doc) {

    validateStructAnnotation(type, schemaString, msg);
    return SchemaUtil.ensureOptional(getSchemaFromInputParameter(type, schemaString, name, doc));
  }

  private static Schema getSchemaFromInputParameter(final Type type,
                                                    final Optional<String> paramSchema,
                                                    final String paramName,
                                                    final String paramDoc) {

    Objects.requireNonNull(paramName);
    Objects.requireNonNull(paramDoc);
    if (paramSchema.isPresent()) {
      return SchemaConverters.sqlToConnectConverter().toConnectSchema(
          typeParser.parse(paramSchema.get()).getSqlType(), paramName, paramDoc);
    }

    return UdfUtil.getSchemaFromType(type, paramName, paramDoc);
  }

  private static void validateUdfInputParameters(final Method method) {

    final Class<?>[] types = method.getParameterTypes();
    for (int i = 0; i < types.length; i++) {
      final Class<?> type = types[i];

      if (method.getGenericParameterTypes()[i] instanceof TypeVariable
          || method.getGenericParameterTypes()[i] instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (!UdfCompiler.isUnsupportedType(type)) {
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
  }

  private static void validateUdafInputParameters(final Method method) {

    final AnnotatedParameterizedType annotatedReturnType
        = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
    final ParameterizedType parameterizedType = (ParameterizedType) annotatedReturnType.getType();

    final Type[] types = parameterizedType.getActualTypeArguments();
    for (int i = 0; i < types.length; i++) {
      final Type type = types[i];

      if (type instanceof TypeVariable || type instanceof GenericArrayType) {
        // this is the case where the type parameter is generic
        continue;
      }

      if (!UdfCompiler.isUnsupportedType((Class<?>) getRawType(type))) {
        throw new KsqlException(
            String.format(
                "Type %s is not supported by UDAF methods. "
                    + "Supported types %s. method=%s, class=%s",
                type,
                SUPPORTED_TYPES,
                method.getName(),
                method.getDeclaringClass()
            )
        );
      }
    }
  }

  private static void validateStructAnnotation(final Type type,
                                               final Optional<String> schema,
                                               final String msg) {

    if (type.equals(Struct.class) && !schema.isPresent()) {
      throw new KsqlException(String.format("Must specify '%s' for STRUCT parameter in "
                                                + "@UdafFactory.", msg));
    }
  }

  private static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  private static void validateUdafMethodSignature(final Method method, final String functionName) {
    final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
                                              method.getName(),
                                              functionName,
                                              method.getDeclaringClass());

    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
                                  + TableUdaf.class.getName() + " ."
                                  + functionInfo);
    }
  }


  private static void validateUdfMethodSignature(final Method method) {
    for (int i = 0; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i].isArray()) {
        if (!method.isVarArgs() || i != method.getParameterCount() - 1) {
          throw new KsqlFunctionException(
              "Invalid UDF method signature (contains non var-arg array): " + method);
        }
      }
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
