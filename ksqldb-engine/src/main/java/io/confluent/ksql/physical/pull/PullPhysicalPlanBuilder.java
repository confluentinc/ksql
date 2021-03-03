/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.physical.pull;

import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.physical.pull.operators.KeyedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.KeyedWindowedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.ProjectOperator;
import io.confluent.ksql.physical.pull.operators.SelectOperator;
import io.confluent.ksql.physical.pull.operators.TableScanOperator;
import io.confluent.ksql.physical.pull.operators.WindowedTableScanOperator;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.LookupConstraint;
import io.confluent.ksql.planner.plan.NonKeyConstraint;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PullFilterNode;
import io.confluent.ksql.planner.plan.PullProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Traverses the logical plan top-down and creates a physical plan for pull queries.
 * The pull query must access a table that is materialized in a state store.
 * The logical plan should consist of Project, Filter and DataSource nodes only.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("UnstableApiUsage")
public class PullPhysicalPlanBuilder {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final ProcessingLogContext processingLogContext;
  private final Stacker contextStacker;
  private final PersistentQueryMetadata persistentQueryMetadata;
  private final QueryId queryId;
  private final Materialization mat;

  private List<LookupConstraint> lookupConstraints;
  private boolean seenSelectOperator = false;

  public PullPhysicalPlanBuilder(
      final ProcessingLogContext processingLogContext,
      final PersistentQueryMetadata persistentQueryMetadata,
      final ImmutableAnalysis analysis
  ) {
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext, "processingLogContext");
    this.persistentQueryMetadata = Objects.requireNonNull(
        persistentQueryMetadata, "persistentQueryMetadata");
    this.contextStacker = new Stacker();
    queryId = uniqueQueryId();
    mat = this.persistentQueryMetadata
        .getMaterialization(queryId, contextStacker)
        .orElseThrow(() -> notMaterializedException(getSourceName(analysis)));
  }

  /**
   * Visits the logical plan top-down to build the physical plan.
   * @param logicalPlanNode the logical plan root node
   * @return the root node of the tree of physical operators
   */
  public PullPhysicalPlan buildPullPhysicalPlan(final LogicalPlanNode logicalPlanNode) {
    DataSourceOperator dataSourceOperator = null;

    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    if (!(outputNode instanceof KsqlBareOutputNode)) {
      throw new KsqlException("Pull queries expect the root of the logical plan to be a "
                                  + "KsqlBareOutputNode.");
    }
    // We skip KsqlBareOutputNode in the translation since it only applies the LIMIT
    PlanNode currentLogicalNode = outputNode.getSource();
    AbstractPhysicalOperator prevPhysicalOp = null;
    AbstractPhysicalOperator rootPhysicalOp = null;
    while (true) {
      AbstractPhysicalOperator currentPhysicalOp = null;
      if (currentLogicalNode instanceof PullProjectNode) {
        currentPhysicalOp = translateProjectNode((PullProjectNode)currentLogicalNode);
      } else if (currentLogicalNode instanceof PullFilterNode) {
        currentPhysicalOp = translateFilterNode((PullFilterNode) currentLogicalNode);
        seenSelectOperator = true;
      } else if (currentLogicalNode instanceof DataSourceNode) {
        currentPhysicalOp = translateDataSourceNode(
            (DataSourceNode) currentLogicalNode);
        dataSourceOperator = (DataSourceOperator)currentPhysicalOp;
      } else {
        throw new KsqlException(String.format(
            "Error in translating logical to physical plan for pull queries: unrecognized logical"
                + " node %s.", currentLogicalNode));
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
        throw new KsqlException("Pull queries do not support joins or nested sub-queries yet.");
      }
      currentLogicalNode = currentLogicalNode.getSources().get(0);
    }

    if (dataSourceOperator == null) {
      throw new IllegalStateException("DataSourceOperator cannot be null in Pull physical plan");
    }
    return new PullPhysicalPlan(
        rootPhysicalOp,
        (rootPhysicalOp).getLogicalNode().getSchema(),
        queryId,
        lookupConstraints,
        mat,
        dataSourceOperator);
  }

  private ProjectOperator translateProjectNode(final PullProjectNode logicalNode) {
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

  private SelectOperator translateFilterNode(final PullFilterNode logicalNode) {
    lookupConstraints = logicalNode.getLookupConstraints();

    final ProcessingLogger logger = processingLogContext
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                QueryType.PULL_QUERY, contextStacker.push("SELECT").getQueryContext())
        );
    return new SelectOperator(logicalNode, logger);
  }

  private AbstractPhysicalOperator translateDataSourceNode(
      final DataSourceNode logicalNode
  ) {
    boolean isTableScan = false;
    if (!seenSelectOperator) {
      lookupConstraints = Collections.emptyList();
      isTableScan = true;
    } else if (lookupConstraints.stream().anyMatch(lc -> lc instanceof NonKeyConstraint)) {
      isTableScan = true;
    }
    if (isTableScan) {
      if (!logicalNode.isWindowed()) {
        return new TableScanOperator(mat, logicalNode);
      } else {
        return new WindowedTableScanOperator(mat, logicalNode);
      }
    }
    if (!logicalNode.isWindowed()) {
      return new KeyedTableLookupOperator(mat, logicalNode);
    } else {
      return new KeyedWindowedTableLookupOperator(mat, logicalNode);
    }
  }

  private QueryId uniqueQueryId() {
    return new QueryId("query_" + System.currentTimeMillis());
  }

  private KsqlException notMaterializedException(final SourceName sourceTable) {
    final String tableName = sourceTable.text();
    return new KsqlException(
        "The " + sourceTable + " table isn't queryable. To derive a queryable table, "
                + "you can do 'CREATE TABLE QUERYABLE_"
                + tableName
                + " AS SELECT * FROM "
                + tableName
                + "'."
                + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
    );
  }

  private SourceName getSourceName(final ImmutableAnalysis analysis) {
    final DataSource source = analysis.getFrom().getDataSource();
    return source.getName();
  }
}
