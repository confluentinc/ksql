/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution;

import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PreJoinProjectNode;
import io.confluent.ksql.planner.plan.PreJoinRepartitionNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.StreamsBuilder;

public class ExecutionPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;

  public ExecutionPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public ExecutionPlan buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final QueryId queryId,
      final Optional<PlanInfo> oldPlanInfo
  ) {
    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    final PlanBuildContext buildContext = PlanBuildContext.of(
        ksqlConfig,
        serviceContext,
        functionRegistry,
        oldPlanInfo
    );

    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_SELF_JOIN_OPTIMIZATION_ENABLE)) {
      optimizeForSelfJoin(outputNode);
    }

    final SchemaKStream<?> resultStream = outputNode.buildStream(buildContext);

    final LogicalSchema logicalSchema = outputNode.getSchema();
    final LogicalSchema physicalSchema = resultStream.getSchema();
    if (!logicalSchema.equals(physicalSchema)) {
      throw new IllegalStateException("Logical and Physical schemas do not match!"
          + System.lineSeparator()
          + "Logical : " + logicalSchema
          + System.lineSeparator()
          + "Physical: " + physicalSchema
      );
    }

    return new ExecutionPlan(
        queryId,
        resultStream.getSourceStep()
    );
  }


  private void optimizeForSelfJoin(final PlanNode currentNode) {

    if (currentNode instanceof JoinNode) {
      final JoinNode joinNode = (JoinNode) currentNode;
      if (! matchSubGraphPattern1(joinNode)) {
        if (!matchSubGraphPattern2(joinNode)) {
          matchSubGraphPattern3(joinNode);
        }
      }
    }

    for (final PlanNode child: currentNode.getSources()) {
      optimizeForSelfJoin(child);
    }
  }

  /**
   * Join -> PreJoinProject -> PreJoinRepartition -> DataSource
   */
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private boolean matchSubGraphPattern1(final JoinNode joinNode) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (joinNode.getLeft() instanceof PreJoinProjectNode
        && joinNode.getRight() instanceof PreJoinProjectNode) {
      final PreJoinProjectNode preJoinProjectNodeLeft =
          (PreJoinProjectNode) joinNode.getLeft();
      final PreJoinProjectNode preJoinProjectNodeRight =
          (PreJoinProjectNode) joinNode.getRight();
      if (preJoinProjectNodeLeft.getSource() instanceof PreJoinRepartitionNode
          && preJoinProjectNodeRight.getSource() instanceof PreJoinRepartitionNode) {

        final PreJoinRepartitionNode preJoinRepartitionNodeLeft =
            (PreJoinRepartitionNode) preJoinProjectNodeLeft.getSource();
        final PreJoinRepartitionNode preJoinRepartitionNodeRight =
            (PreJoinRepartitionNode) preJoinProjectNodeRight.getSource();

        // Check if Repartition is on the same key
        if (!preJoinRepartitionNodeLeft.getPartitionBy()
            .equals(preJoinRepartitionNodeRight.getPartitionBy())) {
          return false;
        }

        // Check if sources are the same stream
        if (!(preJoinRepartitionNodeLeft.getSource() instanceof DataSourceNode)
            || !(preJoinRepartitionNodeRight.getSource() instanceof DataSourceNode)) {
          return false;
        }
        final DataSourceNode leftSource = (DataSourceNode) preJoinRepartitionNodeLeft.getSource();
        final DataSourceNode rightSource = (DataSourceNode) preJoinRepartitionNodeRight.getSource();

        if (leftSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)
            && rightSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)) {

          if (leftSource.getDataSource().getName().equals(rightSource.getDataSource().getName())) {

            // Annotate join as self-join
            joinNode.setSelfJoin(true);
            preJoinProjectNodeLeft.setSelfJoin(true);
            preJoinProjectNodeRight.setSelfJoin(true);
            return true;
          } else if (leftSource.getDataSource().getKsqlTopic().getKafkaTopicName().equals(
              rightSource.getDataSource().getKsqlTopic().getKafkaTopicName())
              && leftSource.getDataSource().getSchema()
              .equals(rightSource.getDataSource().getSchema())
              && leftSource.getSchema().equals(rightSource.getSchema())
          ) {
            // Annotate join as self-join
            joinNode.setSelfJoin(true);
            preJoinProjectNodeLeft.setSelfJoin(true);
            preJoinProjectNodeRight.setSelfJoin(true);
            return true;
          }
        }
      } else {
        return false;
      }
    }

    return false;
  }

  /**
   * Join -> PreJoinProject -> DataSource
   */
  private boolean matchSubGraphPattern2(final JoinNode joinNode) {
    if (joinNode.getLeft() instanceof PreJoinProjectNode
        && joinNode.getRight() instanceof PreJoinProjectNode) {
      final PreJoinProjectNode preJoinProjectNodeLeft =
          (PreJoinProjectNode) joinNode.getLeft();
      final PreJoinProjectNode preJoinProjectNodeRight =
          (PreJoinProjectNode) joinNode.getRight();
      if (preJoinProjectNodeLeft.getSource() instanceof DataSourceNode
          && preJoinProjectNodeRight.getSource() instanceof DataSourceNode) {

        final DataSourceNode leftSource = (DataSourceNode) preJoinProjectNodeLeft.getSource();
        final DataSourceNode rightSource = (DataSourceNode) preJoinProjectNodeRight.getSource();

        if (leftSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)
            && rightSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)
            && leftSource.getDataSource().getName().equals(rightSource.getDataSource().getName())) {

          // Annotate join as self-join
          joinNode.setSelfJoin(true);
          preJoinProjectNodeLeft.setSelfJoin(true);
          preJoinProjectNodeRight.setSelfJoin(true);
          return true;
        }
      } else {
        return false;
      }
    }

    return false;
  }

  /**
   * Join -> DataSource
   */
  private boolean matchSubGraphPattern3(final JoinNode joinNode) {
    if (joinNode.getLeft() instanceof DataSourceNode
        && joinNode.getRight() instanceof DataSourceNode) {

      final DataSourceNode leftSource = (DataSourceNode) joinNode.getLeft();
      final DataSourceNode rightSource = (DataSourceNode) joinNode.getRight();

      if (leftSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)
          && rightSource.getDataSource().getDataSourceType().equals(DataSourceType.KSTREAM)
          && leftSource.getDataSource().getName().equals(rightSource.getDataSource().getName())) {

        // Annotate join as self-join
        joinNode.setSelfJoin(true);
        return true;
      }
    }
    return false;
  }
}
