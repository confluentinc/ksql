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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.PluggableUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes methods that have been marked with the Udf or UdfFactory annotation.
 * Each method gets a class generated for it. For Udfs it is an {@link UdfInvoker}.
 * For UDAFs it is a {@link KsqlAggregateFunction}
 */
@Immutable
public class UdfCompiler extends FunctionCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdfCompiler.class);
  private static final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  UdfCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
  }


  KsqlFunction compileFunction(final Method method,
                               final String functionName,
                               final ClassLoader loader,
                               final Class<? extends Kudf> udfClass,
                               final String path,
                               final String sensorName,
                               final Function<List<Schema>, Schema> returnSchema) {

    final Udf udfAnnotation = method.getAnnotation(Udf.class);

    validateMethodSignature(method);
    validateParameters(method);
    final List<Schema> parameters = parseUdfInputParameters(method, functionName);
    final Schema javaReturnSchema = getReturnType(method, udfAnnotation);
    final UdfInvoker udf =  generateCode(method, functionName, loader);
    final Function<KsqlConfig, Kudf> udfFactory = getUdfFactory(method,
                                                                functionName,
                                                                udf,
                                                                metrics,
                                                                sensorName);

    return KsqlFunction.create(
        returnSchema,
        javaReturnSchema,
        parameters,
        functionName,
        udfClass,
        udfFactory,
        udfAnnotation.description(),
        path,
        method.isVarArgs());
  }

  @VisibleForTesting
  public static UdfInvoker generateCode(final Method method,
                                        final String functionName,
                                        final ClassLoader loader) {
    try {
      final IScriptEvaluator scriptEvaluator = createScriptEvaluator(method,
          loader,
          method.getDeclaringClass().getName());
      final String code = UdfTemplate.generateCode(method, "thiz");

      LOGGER.trace("Generated class for functionName={}, method={} class\n{}\n",
                   functionName,
                   method.getName(),
                   code);

      return (UdfInvoker) scriptEvaluator.createFastEvaluator(code,
          UdfInvoker.class, new String[]{"thiz", "args"});
    } catch (final KsqlException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile udf wrapper class for "
          + method, e);
    }
  }

  private static Function<KsqlConfig, Kudf> getUdfFactory(final Method method,
                                                          final String functionName,
                                                          final UdfInvoker udf,
                                                          final Optional<Metrics> metrics,
                                                          final String sensorName) {
    return ksqlConfig -> {
      final Object actualUdf = instantiateUdfClass(method, functionName);
      if (actualUdf instanceof Configurable) {
        ((Configurable)actualUdf)
            .configure(ksqlConfig.getKsqlFunctionsConfigProps(functionName));
      }
      final PluggableUdf theUdf = new PluggableUdf(udf, actualUdf, method);
      return metrics.<Kudf>map(m -> new UdfMetricProducer(m.getSensor(sensorName),
                                                          theUdf,
                                                          Time.SYSTEM)).orElse(theUdf);
    };
  }

  private static Object instantiateUdfClass(final Method method,
                                            final String functioName) {
    try {
      return method.getDeclaringClass().newInstance();
    } catch (final Exception e) {
      throw new KsqlException("Failed to create instance for UDF="
                                  + functioName
                                  + ", method=" + method,
                              e);
    }
  }

  private Schema getReturnType(final Method method, final Udf udfAnnotation) {
    try {
      final Schema returnType = udfAnnotation.schema().isEmpty()
          ? UdfUtil.getSchemaFromType(method.getGenericReturnType())
          : SchemaConverters
              .sqlToConnectConverter()
              .toConnectSchema(
                  typeParser.parse(udfAnnotation.schema()).getSqlType());

      return SchemaUtil.ensureOptional(returnType);
    } catch (final KsqlException e) {
      throw new KsqlException("Could not load UDF method with signature: " + method, e);
    }
  }


  static List<Schema> parseUdfInputParameters(final Method method,
                                              final String functionName) {

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
                          idx, functionName, method.getName()));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      final Optional<String> schemaString = annotation.isPresent()
          && !annotation.get().schema().isEmpty()
          ? Optional.of(annotation.get().schema()) : Optional.empty();

      inputSchemas.add(parseParameter(type, schemaString, name, name, doc));
    }
    return inputSchemas;
  }

  static void validateParameters(final Method method) {

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
  }

  static void validateMethodSignature(final Method method) {
    for (int i = 0; i < method.getParameterTypes().length; i++) {
      if (method.getParameterTypes()[i].isArray()) {
        if (!method.isVarArgs() || i != method.getParameterCount() - 1) {
          throw new KsqlFunctionException(
              "Invalid UDF method signature (contains non var-arg array): " + method);
        }
      }
    }
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
