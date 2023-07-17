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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class StreamStreamJoin<K> implements ExecutionStep<KStreamHolder<K>> {

  // keyName was not present before 0.10.0, defaults to legacy ROWKEY
  // This can be removed with the next breaking change.
  public static final String LEGACY_KEY_COL = "ROWKEY";

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final ColumnName keyColName;
  private final Formats leftInternalFormats;
  private final Formats rightInternalFormats;
  private final ExecutionStep<KStreamHolder<K>> leftSource;
  private final ExecutionStep<KStreamHolder<K>> rightSource;
  private final Duration beforeMillis;
  private final Duration afterMillis;

  @SuppressWarnings("unused") // Invoked by reflection
  @JsonCreator
  @Deprecated() // Can be removed at next incompatible version
  private StreamStreamJoin(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "joinType", required = true) final JoinType joinType,
      @JsonProperty(value = "keyName", defaultValue = LEGACY_KEY_COL)
      final Optional<ColumnName> keyColName,
      @JsonProperty(value = "leftInternalFormats", required = true) final Formats leftIntFormats,
      @JsonProperty(value = "rightInternalFormats", required = true) final Formats rightIntFormats,
      @JsonProperty(value = "leftSource", required = true)
      final ExecutionStep<KStreamHolder<K>> leftSource,
      @JsonProperty(value = "rightSource", required = true)
      final ExecutionStep<KStreamHolder<K>> rightSource,
      @JsonProperty(value = "beforeMillis", required = true) final Duration beforeMillis,
      @JsonProperty(value = "afterMillis", required = true) final Duration afterMillis
  ) {
    this(
        props,
        joinType,
        keyColName.orElse(ColumnName.of(LEGACY_KEY_COL)),
        leftIntFormats,
        rightIntFormats,
        leftSource,
        rightSource,
        beforeMillis,
        afterMillis
    );
  }

  public StreamStreamJoin(
      final ExecutionStepPropertiesV1 props,
      final JoinType joinType,
      final ColumnName keyColName,
      final Formats leftIntFormats,
      final Formats rightIntFormats,
      final ExecutionStep<KStreamHolder<K>> leftSource,
      final ExecutionStep<KStreamHolder<K>> rightSource,
      final Duration beforeMillis,
      final Duration afterMillis
  ) {
    this.properties = requireNonNull(props, "props");
    this.leftInternalFormats = requireNonNull(leftIntFormats, "leftIntFormats");
    this.rightInternalFormats = requireNonNull(rightIntFormats, "rightIntFormats");
    this.joinType = requireNonNull(joinType, "joinType");
    this.keyColName = requireNonNull(keyColName, "keyColName");
    this.leftSource = requireNonNull(leftSource, "leftSource");
    this.rightSource = requireNonNull(rightSource, "rightSource");
    this.beforeMillis = requireNonNull(beforeMillis, "beforeMillis");
    this.afterMillis = requireNonNull(afterMillis, "afterMillis");
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

  public ColumnName getKeyColName() {
    return keyColName;
  }

  public Duration getAfterMillis() {
    return afterMillis;
  }

  public Duration getBeforeMillis() {
    return beforeMillis;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamStreamJoin(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamStreamJoin(this);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamStreamJoin<?> that = (StreamStreamJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(keyColName, that.keyColName)
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
        keyColName,
        leftInternalFormats,
        rightInternalFormats,
        leftSource,
        rightSource,
        beforeMillis,
        afterMillis
    );
  }
}
