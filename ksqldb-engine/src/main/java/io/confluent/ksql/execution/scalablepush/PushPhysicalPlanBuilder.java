/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.scalablepush;

import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.common.operators.ProjectOperator;
import io.confluent.ksql.execution.common.operators.SelectOperator;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry.CatchupMetadata;
import io.confluent.ksql.execution.scalablepush.operators.PeekStreamOperator;
import io.confluent.ksql.execution.scalablepush.operators.PushDataSourceOperator;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PushOffsetRange;
import io.vertx.core.Context;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Traverses the logical plan top-down and creates a physical plan for scalable push queries.
 * The logical plan should consist of Project, Filter and DataSource nodes only.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class PushPhysicalPlanBuilder {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final ProcessingLogContext processingLogContext;
  private final PersistentQueryMetadata persistentQueryMetadata;
  private final Stacker contextStacker;
  private final QueryId queryId;

  private QuerySourceType querySourceType;

  public PushPhysicalPlanBuilder(
      final ProcessingLogContext processingLogContext,
      final PersistentQueryMetadata persistentQueryMetadata
  ) {
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext, "processingLogContext");
    this.persistentQueryMetadata = Objects.requireNonNull(
        persistentQueryMetadata, "persistentQueryMetadata");
    this.contextStacker = new Stacker();
    queryId = uniqueQueryId();
  }

  /**
   * Visits the logical plan top-down to build the physical plan.
   * @param logicalPlanNode the logical plan root node
   * @return the root node of the tree of physical operators
   */
  public PushPhysicalPlan buildPushPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final Context context,
      final Optional<PushOffsetRange> offsetRange,
      final Optional<String> catchupConsumerGroup
  ) {
    final String catchupConsumerGroupId = getConsumerGroupId(catchupConsumerGroup);
    PushDataSourceOperator dataSourceOperator = null;

    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    if (!(outputNode instanceof KsqlBareOutputNode)) {
      throw new KsqlException("Push queries expect the root of the logical plan to be a "
          + "KsqlBareOutputNode.");
    }
    // We skip KsqlBareOutputNode in the translation since it only applies the LIMIT
    PlanNode currentLogicalNode = outputNode.getSource();
    AbstractPhysicalOperator prevPhysicalOp = null;
    AbstractPhysicalOperator rootPhysicalOp = null;
    while (true) {
      AbstractPhysicalOperator currentPhysicalOp = null;
      if (currentLogicalNode instanceof QueryProjectNode) {
        currentPhysicalOp = translateProjectNode((QueryProjectNode)currentLogicalNode);
      } else if (currentLogicalNode instanceof QueryFilterNode) {
        currentPhysicalOp = translateFilterNode((QueryFilterNode) currentLogicalNode);
      } else if (currentLogicalNode instanceof DataSourceNode) {
        currentPhysicalOp = translateDataSourceNode(
            (DataSourceNode) currentLogicalNode, offsetRange, catchupConsumerGroupId);
        dataSourceOperator = (PushDataSourceOperator) currentPhysicalOp;
      } else {
        throw new KsqlException(String.format(
            "Error in translating logical to physical plan for scalable push queries:"
                + " unrecognized logical node %s.", currentLogicalNode));
      }

      if (prevPhysicalOp == null) {
        rootPhysicalOp = currentPhysicalOp;
      } else {
        prevPhysicalOp.addChild(currentPhysicalOp);
      }
      prevPhysicalOp = currentPhysicalOp;
      // Exit the loop when a leaf node is reached
      if (currentLogicalNode.getSources().isEmpty()) {
        break;
      }
      if (currentLogicalNode.getSources().size() > 1) {
        throw new KsqlException("Push queries do not support joins or nested sub-queries yet.");
      }
      currentLogicalNode = currentLogicalNode.getSources().get(0);
    }

    if (dataSourceOperator == null) {
      throw new IllegalStateException("DataSourceOperator cannot be null in Push physical plan");
    }
    return new PushPhysicalPlan(
        rootPhysicalOp,
        (rootPhysicalOp).getLogicalNode().getSchema(),
        queryId,
        catchupConsumerGroupId,
        dataSourceOperator.getScalablePushRegistry(),
        dataSourceOperator,
        context,
        querySourceType);
  }

  private ProjectOperator translateProjectNode(final QueryProjectNode logicalNode) {
    final ProcessingLogger logger = processingLogContext
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                QueryType.PULL_QUERY, contextStacker.push("PROJECT").getQueryContext())
        );

    return new ProjectOperator(
        logger,
        logicalNode
    );
  }

  private SelectOperator translateFilterNode(final QueryFilterNode logicalNode) {
    final ProcessingLogger logger = processingLogContext
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                QueryType.PULL_QUERY, contextStacker.push("SELECT").getQueryContext())
        );
    return new SelectOperator(logicalNode, logger);
  }

  private AbstractPhysicalOperator translateDataSourceNode(
      final DataSourceNode logicalNode,
      final Optional<PushOffsetRange> offsetRange,
      final String catchupConsumerGroupId
  ) {
    final ScalablePushRegistry scalablePushRegistry =
        persistentQueryMetadata.getScalablePushRegistry()
        .orElseThrow(() -> new IllegalStateException("Scalable push registry cannot be found"));
    querySourceType = logicalNode.isWindowed()
        ? QuerySourceType.WINDOWED : QuerySourceType.NON_WINDOWED;
    final Optional<CatchupMetadata> catchupMetadata = offsetRange.map(or ->
        new CatchupMetadata(or, catchupConsumerGroupId));
    return new PeekStreamOperator(scalablePushRegistry, logicalNode, queryId, catchupMetadata);
  }

  private String getConsumerGroupId(final Optional<String> catchupConsumerGroupFromSource) {
    final ScalablePushRegistry scalablePushRegistry =
        persistentQueryMetadata.getScalablePushRegistry()
            .orElseThrow(() -> new IllegalStateException("Scalable push registry cannot be found"));
    return catchupConsumerGroupFromSource.orElse(
        scalablePushRegistry.getCatchupConsumerId(UUID.randomUUID().toString()));
  }

  private QueryId uniqueQueryId() {
    return new QueryId("SCALABLE_PUSH_QUERY_" + UUID.randomUUID());
  }
}
