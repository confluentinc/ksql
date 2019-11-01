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

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class KsqlFunction extends KsqlFunctionBase {

  static final String INTERNAL_PATH = "internal";

  private final Class<? extends Kudf> kudfClass;
  private final Function<KsqlConfig, Kudf> udfFactory;

  private KsqlFunction(
      final Function<List<Schema>,Schema> returnSchemaProvider,
      final Schema javaReturnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic) {
    super(returnSchemaProvider, javaReturnType, arguments, functionName, description,
        pathLoadedFrom, isVariadic
    );
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
  }

  /**
   * Create built in / legacy function.
   */
  @SuppressWarnings("deprecation")  // Solution only available in Java9.
  public static KsqlFunction createLegacyBuiltIn(
      final Schema returnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass
  ) {
    final Function<KsqlConfig, Kudf> udfFactory = ksqlConfig -> {
      try {
        return kudfClass.newInstance();
      } catch (final Exception e) {
        throw new KsqlException("Failed to create instance of kudfClass "
            + kudfClass + " for function " + functionName, e);
      }
    };

    return create(
        ignored -> returnType, returnType, arguments, functionName, kudfClass, udfFactory, "",
        INTERNAL_PATH, false);
  }

  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
  }

  /**
   * Create udf.
   *
   * <p>Can be either built-in UDF or true user-supplied.
   */
  static KsqlFunction create(
      final Function<List<Schema>,Schema> schemaProvider,
      final Schema javaReturnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KsqlConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    return new KsqlFunction(
        schemaProvider,
        javaReturnType,
        arguments,
        functionName,
        kudfClass,
        udfFactory,
        description,
        pathLoadedFrom,
        isVariadic);
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