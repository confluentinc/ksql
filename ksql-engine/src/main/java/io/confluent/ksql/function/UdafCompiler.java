/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udaf.UdfArgSupplier;
import io.confluent.ksql.util.KsqlException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.resource.Resource;
import org.codehaus.janino.util.resource.ResourceFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class UdafCompiler extends FunctionCompiler {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(io.confluent.ksql.function.UdafCompiler.class);


  private static final String UDAF_PACKAGE = "io.confluent.ksql.function.udaf.";

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdafCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
  }


  KsqlAggregateFunction<?, ?, ?> compileAggregate(final Method method,
                                                  final ClassLoader loader,
                                                  final String functionName,
                                                  final String description) {

    validateMethodSignature(method, functionName);
    validateParameters(method);

    final UdafTypes types = new UdafTypes(method);
    final Schema inputValue = types.getInputSchema();
    final List<Schema> args = Collections.singletonList(inputValue);
    final Schema aggregateValue = types.getAggregateSchema();
    final Schema returnValue = types.getOutputSchema();

    try {
      final String generatedClassName
          = method.getDeclaringClass().getSimpleName() + "_" + method.getName() + "_Aggregate";
      final String udafClass = UdafTemplate.generateCode(method,
                                                         generatedClassName,
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
                            UdfArgSupplier.class,
                            new String[]{"args", "aggregateType", "outputType", "metrics"});

      return evaluator.apply(args, aggregateValue, returnValue, metrics);
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile KSqlAggregateFunction for method='"
                                  + method.getName() + "' in class='" + method.getDeclaringClass()
                                  + "'", e);
    }
  }

  private static class UdafTypes {

    private final Type inputType;
    private final Type aggregateType;
    private final Type outputType;
    private final Method method;
    private final Optional<UdafFactory> annotation;

    UdafTypes(final Method m) {
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

  static void validateParameters(final Method method) {

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

      if (isUnsupportedType((Class<?>) getRawType(type))) {
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

  static void validateMethodSignature(final Method method, final String functionName) {
    final String functionInfo = String.format("method='%s', functionName='%s', UDFClass='%s'",
                                              method.getName(),
                                              functionName,
                                              method.getDeclaringClass());

    if (!(Udaf.class.equals(method.getReturnType())
        || TableUdaf.class.equals(method.getReturnType()))) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
                                  + TableUdaf.class.getName() + ". "
                                  + functionInfo);
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