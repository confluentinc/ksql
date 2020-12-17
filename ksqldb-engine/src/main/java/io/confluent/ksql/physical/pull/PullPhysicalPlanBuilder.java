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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.physical.pull.operators.KeyedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.KeyedWindowedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.ProjectOperator;
import io.confluent.ksql.physical.pull.operators.SelectOperator;
import io.confluent.ksql.physical.pull.operators.TableScanOperator;
import io.confluent.ksql.physical.pull.operators.WhereInfo;
import io.confluent.ksql.physical.pull.operators.WindowedTableScanOperator;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PullProjectNode;
import io.confluent.ksql.planner.plan.PullFilterNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
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

  private final MetaStore metaStore;
  private final ProcessingLogContext processingLogContext;
  private final KsqlConfig config;
  private final Stacker contextStacker;
  private final ImmutableAnalysis analysis;
  private final ConfiguredStatement<Query> statement;
  private final PersistentQueryMetadata persistentQueryMetadata;
  private final QueryId queryId;
  private final Materialization mat;

  private WhereInfo whereInfo;
  private List<GenericKey> keys;
  //private boolean isWindowed;
  //private WindowBounds windowBounds;

  public PullPhysicalPlanBuilder(
      final MetaStore metaStore,
      final ProcessingLogContext processingLogContext,
      final PersistentQueryMetadata persistentQueryMetadata,
      final KsqlConfig config,
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statement
  ) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext, "processingLogContext");
    this.persistentQueryMetadata = Objects.requireNonNull(
        persistentQueryMetadata, "persistentQueryMetadata");
    this.config = Objects.requireNonNull(config, "config");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.contextStacker = new Stacker();
    this.statement = Objects.requireNonNull(statement, "statement");
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

    // Basic validation, should be moved to logical plan builder
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
        keys,
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
    final boolean windowed = persistentQueryMetadata.getResultTopic().getKeyFormat().isWindowed();
    //isWindowed = logicalNode.isWindowed();
    keys = logicalNode.getKeyValues().stream()
        .map(GenericKey::genericKey)
        .collect(ImmutableList.toImmutableList());
    //windowBounds = logicalNode.getWindowBounds();


    whereInfo = WhereInfo.extractWhereInfo(
        analysis.getWhereExpression().orElseThrow(
            () -> WhereInfo.invalidWhereClauseException("Missing WHERE clause", windowed)),
        persistentQueryMetadata.getLogicalSchema(),
        windowed,
        metaStore,
        config);

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
    final boolean windowed = persistentQueryMetadata.getResultTopic().getKeyFormat().isWindowed();
    if (whereInfo == null) {
      if (!config.getBoolean(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED)) {
        throw WhereInfo.invalidWhereClauseException("Missing WHERE clause", windowed);
      }
      // Full table scan has no keys
      keys = Collections.emptyList();
    } else {
      keys = whereInfo.getKeysBound();
    }

    if (keys.isEmpty()) {
      if (!windowed) {
        return new TableScanOperator(mat, logicalNode);
      } else {
        return new WindowedTableScanOperator(mat, logicalNode);
      }
    } else if (!windowed) {
      return new KeyedTableLookupOperator(mat, logicalNode);
    } else {
      return new KeyedWindowedTableLookupOperator(
          mat, logicalNode, whereInfo.getWindowBounds().get());
    }
  }

  private QueryId uniqueQueryId() {
    return new QueryId("query_" + System.currentTimeMillis());
  }

  private KsqlException notMaterializedException(final SourceName sourceTable) {
    return new KsqlException(
        "Can't pull from " + sourceTable + " as it's not a materialized table."
            + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
    );
  }

  private SourceName getSourceName(final ImmutableAnalysis analysis) {
    final DataSource source = analysis.getFrom().getDataSource();
    return source.getName();
  }
}
