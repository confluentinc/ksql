/*
 * Copyright 2022 Confluent Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import java.util.List;
import java.util.Optional;

@Immutable
public class StreamNoOpPreJoinSelect<K>
    extends StreamSelect<K>
    implements ExecutionStep<KStreamHolder<K>> {

  public StreamNoOpPreJoinSelect(
      final ExecutionStepPropertiesV1 props,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<ColumnName> keyColumnNames,
      final Optional<List<ColumnName>> selectedKeys,
      final List<SelectExpression> selectExpressions
  ) {
    super(props, source, keyColumnNames, selectedKeys, selectExpressions);
  }

  /**
   * This constructor is required while {@code keyColumnNames} are not mandatory.
   *
   * <p>{@code keyColumnNames} was introduced in 0.10.0 and can be mandatory once 0.9.0 query plans
   * are no longer supported.
   *
   * @see <a href="https://github.com/confluentinc/ksql/issues/5420">Tracking issue</a>
   * @deprecated use the public constructor.
   */
  @SuppressWarnings("unused") // Invoked via reflection by Jackson
  @JsonCreator
  @Deprecated
  private StreamNoOpPreJoinSelect(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "keyColumnNames") final Optional<List<ColumnName>> keyColumnNames,
      @JsonProperty(value = "selectedKeys") final Optional<List<ColumnName>> selectedKeys,
      @JsonProperty(value = "selectExpressions", required = true) final
      List<SelectExpression> selectExpressions
  ) {
    this(
        props,
        source,
        keyColumnNames.orElseGet(ImmutableList::of),
        selectedKeys,
        selectExpressions
    );
  }

  @Override
  public KStreamHolder<K> build(
      final PlanBuilder builder, final PlanInfo info, final boolean isSelfJoin) {
    return builder.visitStreamNoOpPreJoinSelect(this, info, isSelfJoin);
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamNoOpPreJoinSelect(this, info, true);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamNoOpSelect(this);
  }
}
