/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamStreamJoin<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final Formats leftInternalFormats;
  private final Formats rightInternalFormats;
  private final ExecutionStep<KStreamHolder<K>> leftSource;
  private final ExecutionStep<KStreamHolder<K>> rightSource;
  private final Duration beforeMillis;
  private final Duration afterMillis;

  public StreamStreamJoin(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "joinType", required = true) JoinType joinType,
      @JsonProperty(value = "leftInternalFormats", required = true) Formats leftInternalFormats,
      @JsonProperty(value = "rightInternalFormats", required = true) Formats rightInternalFormats,
      @JsonProperty(value = "leftSource", required = true)
      ExecutionStep<KStreamHolder<K>> leftSource,
      @JsonProperty(value = "rightSource", required = true)
      ExecutionStep<KStreamHolder<K>> rightSource,
      @JsonProperty(value = "beforeMillis", required = true) Duration beforeMillis,
      @JsonProperty(value = "afterMillis", required = true) Duration afterMillis) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.leftInternalFormats =
        Objects.requireNonNull(leftInternalFormats, "leftInternalFormats");
    this.rightInternalFormats =
        Objects.requireNonNull(rightInternalFormats, "rightInternalFormats");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.leftSource = Objects.requireNonNull(leftSource, "leftSource");
    this.rightSource = Objects.requireNonNull(rightSource, "rightSource");
    this.beforeMillis = Objects.requireNonNull(beforeMillis, "beforeMillis");
    this.afterMillis = Objects.requireNonNull(afterMillis, "afterMillis");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(leftSource, rightSource);
  }

  public Formats getLeftInternalFormats() {
    return leftInternalFormats;
  }

  public Formats getRightInternalFormats() {
    return rightInternalFormats;
  }

  public ExecutionStep<KStreamHolder<K>> getLeftSource() {
    return leftSource;
  }

  public ExecutionStep<KStreamHolder<K>> getRightSource() {
    return rightSource;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public Duration getAfterMillis() {
    return afterMillis;
  }

  public Duration getBeforeMillis() {
    return beforeMillis;
  }

  @Override
  public KStreamHolder<K> build(PlanBuilder builder) {
    return builder.visitStreamStreamJoin(this);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamStreamJoin<?> that = (StreamStreamJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(leftInternalFormats, that.leftInternalFormats)
        && Objects.equals(rightInternalFormats, that.rightInternalFormats)
        && Objects.equals(leftSource, that.leftSource)
        && Objects.equals(rightSource, that.rightSource)
        && Objects.equals(beforeMillis, that.beforeMillis)
        && Objects.equals(afterMillis, that.afterMillis);
  }
  // CHECKSTYLE_RULES.ON: CyclomaticComplexity

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        joinType,
        leftInternalFormats,
        rightInternalFormats,
        leftSource,
        rightSource,
        beforeMillis,
        afterMillis
    );
  }
}
