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

import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
public final class KsqlScalarFunction extends KsqlFunction {

  public static final String INTERNAL_PATH = "internal";

  private final Class<? extends Kudf> kudfClass;
  @EffectivelyImmutable
  private final Function<KsqlConfig, Kudf> udfFactory;

  private KsqlScalarFunction(
      final SchemaProvider returnSchemaProvider,
      final ParamType javaReturnType,
      final List<ParameterInfo> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    super(returnSchemaProvider, javaReturnType, arguments, functionName, description,
        pathLoadedFrom, isVariadic
    );
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
  }

  /**
   * Create built in / legacy function.
   */
  public static KsqlScalarFunction createLegacyBuiltIn(
      final SqlType returnType,
      final List<ParamType> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass
  ) {
    final ParamType javaReturnType = SchemaConverters.sqlToFunctionConverter()
        .toFunctionType(returnType);

    final List<ParameterInfo> paramInfos = arguments
        .stream()
        .map(type -> new ParameterInfo("", type, "", false))
        .collect(Collectors.toList());

    return create(
        (i1, i2) -> returnType,
        javaReturnType,
        paramInfos,
        functionName,
        kudfClass,
        // findbugs was complaining about a dead store so I inlined this
        ksqlConfig -> {
          try {
            return kudfClass.newInstance();
          } catch (final Exception e) {
            throw new KsqlException("Failed to create instance of kudfClass "
                + kudfClass + " for function " + functionName, e);
          }
        },
        "",
        INTERNAL_PATH, false
    );
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
  }

  /**
   * Create udf.
   *
   * <p>Can be either built-in UDF or true user-supplied.
   */
  static KsqlScalarFunction create(
      final SchemaProvider schemaProvider,
      final ParamType javaReturnType,
      final List<ParameterInfo> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    return new KsqlScalarFunction(
        schemaProvider,
        javaReturnType,
        arguments,
        functionName,
        kudfClass,
        udfFactory,
        description,
        pathLoadedFrom,
        isVariadic
    );
  }

  @Override
  public String toString() {
    return "KsqlFunction{"
        + ", kudfClass=" + kudfClass
        + '}';
  }

  public Kudf newInstance(final KsqlConfig ksqlConfig) {
    return udfFactory.apply(ksqlConfig);
  }
}