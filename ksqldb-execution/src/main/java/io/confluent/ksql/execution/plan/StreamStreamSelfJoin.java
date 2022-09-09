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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.time.Duration;
import java.util.Optional;

@Immutable
public class StreamStreamSelfJoin<K>
    extends StreamStreamJoin<K>
    implements ExecutionStep<KStreamHolder<K>> {

  // keyName was not present before 0.10.0, defaults to legacy ROWKEY
  // This can be removed with the next breaking change.
  public static final String LEGACY_KEY_COL = "ROWKEY";


  @SuppressWarnings("unused") // Invoked by reflection
  @JsonCreator
  @Deprecated() // Can be removed at next incompatible version
  private StreamStreamSelfJoin(
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
      @JsonProperty(value = "afterMillis", required = true) final Duration afterMillis,
      @JsonProperty(value = "graceMillis") final Optional<Duration> graceMillis
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
        afterMillis,
        graceMillis
    );
  }

  public StreamStreamSelfJoin(
      final ExecutionStepPropertiesV1 props,
      final JoinType joinType,
      final ColumnName keyColName,
      final Formats leftIntFormats,
      final Formats rightIntFormats,
      final ExecutionStep<KStreamHolder<K>> leftSource,
      final ExecutionStep<KStreamHolder<K>> rightSource,
      final Duration beforeMillis,
      final Duration afterMillis,
      final Optional<Duration> graceMillis
  ) {
    super(props, joinType, keyColName, leftIntFormats, rightIntFormats, leftSource, rightSource,
          beforeMillis, afterMillis, graceMillis);
  }


  @Override
  public KStreamHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamStreamSelfJoin(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamStreamSelfJoin(this);
  }
}
