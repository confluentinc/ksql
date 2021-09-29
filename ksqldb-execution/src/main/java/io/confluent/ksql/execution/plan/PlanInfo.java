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

package io.confluent.ksql.execution.plan;

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Information about an execution step plan to be passed to a {@link PlanBuilder}
 * for use when converting an execution step plan into a physical plan.
 *
 * <p>Today, the information stored includes all source steps and whether each is
 * repartitioned at any point prior to a join. This information is used by the
 * {@code KSPlanBuilder} to determine whether to force materialization on table sources.
 * If a table source is repartitioned downstream prior to a join, then we do not need
 * to materialize the source table as the join will create a state store from the
 * repartition topic rather than the source topic.
 * See https://github.com/confluentinc/ksql/issues/6650 and the use of
 * {@code PlanInfo} in {@code SourceBuilder} for more.
 */
public class PlanInfo {

  private final Sources allSources;
  /**
   * Present iff the execution step this plan info corresponds has exactly one
   * upstream source node (i.e., no joins have been encountered thus far).
   * If so, this tracks the single source node. Else, empty.
   */
  private final Optional<SourceInfo> activeSource;

  public PlanInfo(final ExecutionStep<?> sourceStep) {
    this.allSources = new Sources();
    final SourceInfo sourceInfo = allSources.addSource(sourceStep);
    this.activeSource = Optional.of(sourceInfo);
  }

  private PlanInfo(final Sources sources) {
    this.allSources = sources;
    this.activeSource = Optional.empty();
  }

  public boolean isRepartitionedInPlan(final ExecutionStep<?> sourceStep) {
    final SourceInfo sourceInfo = allSources.get(sourceStep);
    if (sourceInfo == null) {
      throw new IllegalStateException("Source not found");
    }
    return sourceInfo.isRepartitionedInPlan;
  }

  public PlanInfo setIsRepartitionedInPlan() {
    activeSource.ifPresent(sourceInfo -> sourceInfo.isRepartitionedInPlan = true);
    return this;
  }

  public PlanInfo merge(final PlanInfo other) {
    return new PlanInfo(allSources.merge(other.allSources));
  }

  public Set<ExecutionStep<?>> getSources() {
    return allSources.sourceSet;
  }

  private static class SourceInfo {
    final ExecutionStep<?> sourceStep;
    boolean isRepartitionedInPlan;

    SourceInfo(final ExecutionStep<?> sourceStep) {
      this.sourceStep = Objects.requireNonNull(sourceStep);
    }
  }

  /**
   * Map of source steps to information including whether the source is repartitioned
   * downstream (prior to a join).
   */
  private static class Sources {
    private final HashMap<ExecutionStep<?>, SourceInfo> sources = new HashMap<>();

    Set<ExecutionStep<?>> sourceSet = sources.keySet();

    SourceInfo addSource(final ExecutionStep<?> sourceStep) {
      final SourceInfo sourceInfo = new SourceInfo(sourceStep);
      sources.put(sourceStep, sourceInfo);
      return sourceInfo;
    }

    SourceInfo get(final ExecutionStep<?> sourceStep) {
      return sources.get(sourceStep);
    }

    Sources merge(final Sources other) {
      sources.putAll(other.sources);
      return this;
    }
  }
}
