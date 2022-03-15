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
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.ExtensionSecurityManager;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for loading different types of user defined funcrions
 */
public final class FunctionLoaderUtils {

  private FunctionLoaderUtils() {
  }

  static List<ParameterInfo> createParameters(
      final Method method, final String functionName,
      final SqlTypeParser typeParser
  ) {
    return IntStream.range(0, method.getParameterCount()).mapToObj(idx -> {
      final Type type = method.getGenericParameterTypes()[idx];
      final Optional<UdfParameter> annotation = Arrays.stream(method.getParameterAnnotations()[idx])
          .filter(UdfParameter.class::isInstance)
          .map(UdfParameter.class::cast)
          .findAny();

      final Parameter param = method.getParameters()[idx];
      final String name = annotation.map(UdfParameter::value)
          .filter(val -> !val.isEmpty())
          .orElse(param.isNamePresent() ? param.getName() : "");

      if (name.trim().isEmpty()) {
        throw new KsqlFunctionException(
            String.format("Cannot resolve parameter name for param at index %d for UDF %s:%s. "
                    + "Please specify a name in @UdfParameter or compile your JAR with -parameters "
                    + "to infer the name from the parameter name.",
                idx, functionName, method.getName()
            ));
      }

      final ParamType paramType;
      if (annotation.isPresent() && !annotation.get().schema().isEmpty()) {
        paramType = SchemaConverters.sqlToFunctionConverter()
            .toFunctionType(
                typeParser.parse(annotation.get().schema()).getSqlType()
            );
      } else {
        paramType = UdfUtil.getSchemaFromType(type);
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      final boolean isVariadicParam = idx == method.getParameterCount() - 1 && method.isVarArgs();
      return new ParameterInfo(name, paramType, doc, isVariadicParam);
    }).collect(Collectors.toList());
  }

  @VisibleForTesting
  public static FunctionInvoker createFunctionInvoker(final Method method) {
    return new DynamicFunctionInvoker(method);
  }

  static Object instantiateFunctionInstance(
      final Class functionClass,
      final String functionName
  ) {
    try {
      return functionClass.newInstance();
    } catch (final Exception e) {
      throw new KsqlException(
          "Failed to create instance for UDF/UDTF="
              + functionName,
          e
      );
    }
  }

  static ParamType getReturnType(
      final Method method, final String annotationSchema,
      final SqlTypeParser typeParser
  ) {
    return getReturnType(method, method.getGenericReturnType(), annotationSchema, typeParser);
  }

  static ParamType getReturnType(
      final Method method, final Type type, final String annotationSchema,
      final SqlTypeParser typeParser
  ) {
    try {
      return annotationSchema.isEmpty()
          ? UdfUtil.getSchemaFromType(type)
          : SchemaConverters
              .sqlToFunctionConverter()
              .toFunctionType(
                  typeParser.parse(annotationSchema).getSqlType());
    } catch (final KsqlException e) {
      throw new KsqlException("Could not load UDF method with signature: " + method, e);
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  static SchemaProvider handleUdfReturnSchema(
      final Class theClass,
      final ParamType javaReturnSchema,
      final String annotationSchema,
      final SqlTypeParser parser,
      final String schemaProviderFunctionName,
      final String functionName,
      final boolean isVariadic
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    final Function<List<SqlArgument>, SqlType> schemaProvider;
    if (!Udf.NO_SCHEMA_PROVIDER.equals(schemaProviderFunctionName)) {
      schemaProvider = handleUdfSchemaProviderAnnotation(
          schemaProviderFunctionName, theClass, functionName);
    } else if (!Udf.NO_SCHEMA.equals(annotationSchema)) {
      final SqlType sqlType = parser.parse(annotationSchema).getSqlType();
      schemaProvider = args -> sqlType;
    } else if (!GenericsUtil.hasGenerics(javaReturnSchema)) {
      // it is important to do this eagerly and not in the lambda so that
      // we can fail early (when loading the UDF) instead of when the user
      // attempts to use the UDF
      final SqlType sqlType = fromJavaType(javaReturnSchema, functionName);
      schemaProvider = args -> sqlType;
    } else {
      schemaProvider = null;
    }

    return (parameters, arguments) -> {
      if (schemaProvider != null) {
        final SqlType returnType = schemaProvider.apply(arguments);
        if (!(ParamTypes.areCompatible(SqlArgument.of(returnType), javaReturnSchema, false))) {
          throw new KsqlException(String.format(
              "Return type %s of UDF %s does not match the declared "
                  + "return type %s.",
              returnType,
              functionName.toUpperCase(),
              SchemaConverters.functionToSqlConverter().toSqlType(javaReturnSchema)
          ));
        }
        return returnType;
      }

      final Map<GenericType, SqlType> genericMapping = new HashMap<>();
      for (int i = 0; i < Math.min(parameters.size(), arguments.size()); i++) {
        final ParamType schema = parameters.get(i);
        if (schema instanceof LambdaType) {
          if (isVariadic && i == parameters.size() - 1) {
            throw new KsqlException(
                String.format(
                    "Lambda function %s cannot be variadic.",
                    arguments.get(i).toString()));
          }
          genericMapping.putAll(GenericsUtil.reserveGenerics(
              schema,
              arguments.get(i)
          ));
        } else {
          // we resolve any variadic as if it were an array so that the type
          // structure matches the input type
          final SqlType instance = isVariadic && i == parameters.size() - 1
              ? SqlTypes.array(arguments.get(i).getSqlTypeOrThrow())
              : arguments.get(i).getSqlTypeOrThrow();
          genericMapping.putAll(
              GenericsUtil.reserveGenerics(
                  schema,
                  SqlArgument.of(instance)
              )
          );
        }
      }

      return GenericsUtil.applyResolved(javaReturnSchema, genericMapping);
    };
  }

  private static SqlType fromJavaType(final ParamType javaReturnSchema, final String functionName) {
    try {
      return SchemaConverters.functionToSqlConverter().toSqlType(javaReturnSchema);
    } catch (final Exception e) {
      throw new KsqlException("Cannot load UDF " + functionName + ". " + javaReturnSchema
          + " return type is not supported without an explicit schema or schema provider set"
          + " in the method annotation.");
    }
  }

  private static Function<List<SqlArgument>, SqlType> handleUdfSchemaProviderAnnotation(
      final String schemaProviderName,
      final Class theClass,
      final String functionName
  ) {
    // throws exception if it cannot find the method
    final Method m = findSchemaProvider(theClass, schemaProviderName);
    final Object instance = FunctionLoaderUtils
        .instantiateFunctionInstance(theClass, functionName);

    return
        parameterSchemas -> invokeSchemaProviderMethod(instance, m, parameterSchemas, functionName);
  }

  private static Method findSchemaProvider(
      final Class<?> theClass,
      final String schemaProviderName
  ) {
    try {
      final Method m = theClass.getDeclaredMethod(schemaProviderName, List.class);
      if (!m.isAnnotationPresent(UdfSchemaProvider.class)) {
        throw new KsqlException(String.format(
            "Method %s should be annotated with @UdfSchemaProvider.",
            schemaProviderName
        ));
      }
      return m;
    } catch (final NoSuchMethodException e) {
      throw new KsqlException(String.format(
          "Cannot find schema provider method with name %s and parameter List<SqlType> in class "
              + "%s.", schemaProviderName, theClass.getName()), e);
    }
  }

  private static SqlType invokeSchemaProviderMethod(
      final Object instance,
      final Method m,
      final List<SqlArgument> args,
      final String functionName
  ) {
    try {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      return (SqlType) m.invoke(instance, args);
    } catch (IllegalAccessException
        | InvocationTargetException e) {
      throw new KsqlException(String.format("Cannot invoke the schema provider "
              + "method %s for UDF %s. ",
          m.getName(), functionName
      ), e);
    } finally {
      ExtensionSecurityManager.INSTANCE.popOutUdf();
    }
  }
}
