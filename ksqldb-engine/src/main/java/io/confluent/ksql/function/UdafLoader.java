/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads user defined aggregate functions (UDAFs)
 */
public class UdafLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdafLoader.class);

  private final MutableFunctionRegistry functionRegistry;
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;

  public UdafLoader(
      final MutableFunctionRegistry functionRegistry,
      final Optional<Metrics> metrics,
      final SqlTypeParser typeParser
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.typeParser = Objects.requireNonNull(typeParser, "typeParser");
  }

  public void loadUdafFromClass(final Class<?> theClass, final String path) {
    final UdafDescription udafAnnotation = theClass.getAnnotation(UdafDescription.class);
    
    final List<UdafFactoryInvoker> invokers = new ArrayList<>();
    for (final Method method : theClass.getMethods()) {
      if (method.getAnnotation(UdafFactory.class) == null) {
        continue;
      }

      if (!Modifier.isStatic(method.getModifiers())) {
        LOGGER.warn(
            "Trying to create a UDAF from a non-static factory method. Udaf factory"
                + " methods must be static. class={}, method={}, name={}",
            method.getDeclaringClass(),
            method.getName(),
            udafAnnotation.name()
        );
        continue;
      }

      final UdafFactory annotation = method.getAnnotation(UdafFactory.class);
      try {
        LOGGER.debug(
            "Adding UDAF name={} from path={} class={}",
            udafAnnotation.name(),
            path,
            method.getDeclaringClass()
        );
        final UdafFactoryInvoker invoker = createUdafFactoryInvoker(
            method,
            FunctionName.of(udafAnnotation.name()),
            annotation.description(),
            annotation.paramSchema(),
            annotation.aggregateSchema(),
            annotation.returnSchema()
        );
        invokers.add(invoker);
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to create UDAF name={}, method={}, class={}, path={}",
            udafAnnotation.name(),
            method.getName(),
            method.getDeclaringClass(),
            path,
            e
        );
      }
    }

    functionRegistry.addAggregateFunctionFactory(new UdafAggregateFunctionFactory(
        new UdfMetadata(
            udafAnnotation.name(),
            udafAnnotation.description(),
            udafAnnotation.author(),
            udafAnnotation.version(),
            udafAnnotation.category(),
            path
        ),
        invokers
    ));
  }

  UdafFactoryInvoker createUdafFactoryInvoker(
      final Method method,
      final FunctionName functionName,
      final String description,
      final String[] inputSchemas,
      final String aggregateSchema,
      final String outputSchema
  ) {
    return new UdafFactoryInvoker(method, functionName, description, inputSchemas,
            aggregateSchema, outputSchema, typeParser, metrics
    );
  }
}
