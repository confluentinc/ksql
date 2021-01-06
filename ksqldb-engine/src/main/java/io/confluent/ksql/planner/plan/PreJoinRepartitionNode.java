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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.Repartitioning;
import java.util.Optional;

/**
 * Node to handle an implicit repartition required to enable a join.
 *
 * <p>Any join that is not on the key of the stream requires ksql to perform an
 * implicit repartition step before joining.
 */
public class PreJoinRepartitionNode extends SingleSourcePlanNode implements JoiningNode {

  private final Expression partitionBy;
  private final LogicalSchema schema;
  private final Optional<JoinNode> joiningNode;
  private final ValueFormat valueFormat;
  private Optional<KeyFormat> forcedInternalKeyFormat = Optional.empty();
  private boolean forceRepartition = false;
  private boolean keyFormatSet = false;

  public PreJoinRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression partitionBy
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.schema = requireNonNull(schema, "schema");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");

    if (source instanceof JoiningNode) {
      if (!(source instanceof JoinNode)) {
        throw new IllegalStateException(
            "PreJoinRepartitionNode preceded by non-JoinNode JoiningNode: " + source.getClass());
      }
      this.joiningNode = Optional.of((JoinNode) source);
    } else {
      this.joiningNode = Optional.empty();
    }

    this.valueFormat = JoiningNode.getValueFormatForSource(this);
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public Optional<KeyFormat> getPreferredKeyFormat() {
    if (requiresRepartition()) {
      return Optional.empty();
    }

    if (joiningNode.isPresent()) {
      return joiningNode.get().getPreferredKeyFormat();
    }

    return Optional.of(getSourceKeyFormat());
  }

  @Override
  public void setKeyFormat(final KeyFormat format) {
    final Optional<KeyFormat> requiredParentJoinFormat = maybeForceInternalKeyFormat(format);

    if (joiningNode.isPresent()) {
      if (requiredParentJoinFormat.isPresent()) {
        joiningNode.get().setKeyFormat(requiredParentJoinFormat.get());
      } else {
        joiningNode.get().resolveKeyFormats();
      }
    }

    keyFormatSet = true;
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    if (!keyFormatSet) {
      throw new IllegalStateException("PreJoinRepartitionNode must set key format");
    }

    return getSource().buildStream(buildContext)
        .selectKey(
            valueFormat.getFormatInfo(),
            ImmutableList.of(partitionBy),
            forcedInternalKeyFormat,
            buildContext.buildNodeContext(getId().toString()),
            forceRepartition
        );
  }

  private boolean requiresRepartition() {
    return Repartitioning.repartitionNeeded(getSource().getSchema(), ImmutableList.of(partitionBy));
  }

  // Only safe to call this if joiningNode is empty.
  private KeyFormat getSourceKeyFormat() {
    return Iterators.getOnlyElement(getSourceNodes().iterator())
        .getDataSource().getKsqlTopic().getKeyFormat();
  }

  /**
   * Evaluates whether this node should repartition, based on the desired key format
   *
   * @param format key format being set on this node
   * @return if applicable, the format that must be set on this node's parent JoinNode
   */
  private Optional<KeyFormat> maybeForceInternalKeyFormat(final KeyFormat format) {
    // Force repartition in case of schema inference, to avoid misses due to key schema ID mismatch
    // See https://github.com/confluentinc/ksql/issues/6332 for context, and
    // https://github.com/confluentinc/ksql/issues/6648 for a potential optimization
    if (FormatFactory.of(format.getFormatInfo()).supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
      forceRepartition = true;
    }

    if (requiresRepartition() || forceRepartition) {
      // Node is repartitioning already:
      forcedInternalKeyFormat = Optional.of(format);
      return Optional.empty();
    }

    if (joiningNode.isPresent()) {
      final Optional<KeyFormat> preferred = joiningNode.get().getPreferredKeyFormat();
      if (!preferred.isPresent() || preferred.get().equals(format)) {
        // Parent node can handle any key format change
        return Optional.of(format);
      } else {
        forcedInternalKeyFormat = Optional.of(format);
        return Optional.empty();
      }
    }

    if (!format.equals(getSourceKeyFormat())) {
      forcedInternalKeyFormat = Optional.of(format);
    }

    return Optional.empty();
  }
}
