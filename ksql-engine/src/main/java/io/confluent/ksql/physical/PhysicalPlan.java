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

package io.confluent.ksql.physical;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.query.QueryId;
import java.util.Objects;

@Immutable
public final class PhysicalPlan<T> {
  private final QueryId queryId;
  private final ExecutionStep<T> physicalPlan;
  private final String planSummary;
  private final transient KeyField keyField;

  PhysicalPlan(
      final QueryId queryId,
      final ExecutionStep<T> physicalPlan,
      final String planSummary,
      final KeyField keyField
  ) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlan");
    this.planSummary = Objects.requireNonNull(planSummary, "planSummary");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
  }

  public ExecutionStep<?> getPhysicalPlan() {
    return physicalPlan;
  }

  public String getPlanSummary() {
    return planSummary;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public QueryId getQueryId() {
    return queryId;
  }
}
