/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;

/**
 * Contains all the context required to build an execution plan from a logical plan.
 */
public final class PlanBuildContext {
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;

  public static PlanBuildContext of(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry
  ) {
    return new PlanBuildContext(
        ksqlConfig,
        serviceContext,
        functionRegistry
    );
  }

  private PlanBuildContext(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry
  ) {
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public PlanBuildContext withKsqlConfig(final KsqlConfig newConfig) {
    return of(
        newConfig,
        serviceContext,
        functionRegistry
    );
  }

  @SuppressWarnings("MethodMayBeStatic") // Non-static to allow DI/mocking
  public QueryContext.Stacker buildNodeContext(final String context) {
    return new QueryContext.Stacker()
        .push(context);
  }
}
