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
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatInfo;
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
  private final Optional<JoiningNode> joiningNode;
  private Optional<FormatInfo> forcedInternalKeyFormat = Optional.empty();

  public PreJoinRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression partitionBy
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.schema = requireNonNull(schema, "schema");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
    this.joiningNode = source instanceof JoiningNode
        ? Optional.of((JoiningNode) source)
        : Optional.empty();
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public Optional<RequiredFormat> getRequiredKeyFormat() {
    if (joiningNode.isPresent()) {
      return joiningNode.get().getRequiredKeyFormat();
    }

    final DataSource dataSource = Iterators.getOnlyElement(getSourceNodes().iterator())
        .getDataSource();

    if (dataSource.getDataSourceType() != DataSourceType.KTABLE) {
      return Optional.empty();
    }

    return Optional.of(RequiredFormat.of(getSourceKeyFormat(), dataSource.getName()));
  }

  @Override
  public Optional<FormatInfo> getPreferredKeyFormat() {
    if (requiresRepartition()) {
      return Optional.empty();
    }

    if (joiningNode.isPresent()) {
      return joiningNode.get().getPreferredKeyFormat();
    }

    return Optional.of(getSourceKeyFormat());
  }

  @Override
  public void setKeyFormat(final FormatInfo format) {
    if (requiresRepartition()) {
      // Node is repartitioning already:
      forcedInternalKeyFormat = Optional.of(format);
      return;
    }

    if (joiningNode.isPresent()) {
      final Optional<FormatInfo> preferred = joiningNode.get().getPreferredKeyFormat();
      if (!preferred.isPresent() || preferred.get().equals(format)) {
        // Parent node can handle any key format change:
        joiningNode.get().setKeyFormat(format);
      } else {
        forcedInternalKeyFormat = Optional.of(format);
      }
      return;
    }

    if (!format.equals(getSourceKeyFormat())) {
      forcedInternalKeyFormat = Optional.of(format);
    }
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .selectKey(
            partitionBy,
            forcedInternalKeyFormat,
            builder.buildNodeContext(getId().toString())
        );
  }

  private boolean requiresRepartition() {
    return Repartitioning.repartitionNeeded(getSource().getSchema(), ImmutableList.of(partitionBy));
  }

  // Only safe to call this if joiningNode is empty.
  private FormatInfo getSourceKeyFormat() {
    return Iterators.getOnlyElement(getSourceNodes().iterator())
        .getDataSource().getKsqlTopic().getKeyFormat().getFormatInfo();
  }
}
