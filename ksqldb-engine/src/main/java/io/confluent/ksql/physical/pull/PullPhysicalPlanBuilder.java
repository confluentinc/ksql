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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.pull.operators.DataSourceOperator;
import io.confluent.ksql.physical.pull.operators.KeyedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.KeyedWindowedTableLookupOperator;
import io.confluent.ksql.physical.pull.operators.ProjectOperator;
import io.confluent.ksql.physical.pull.operators.SelectOperator;
import io.confluent.ksql.physical.pull.operators.WhereInfo;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

/**
 * Traverses the logical plan top-down and creates a physical plan for pull queries.
 * The pull query must access a table that is materialized in a state store.
 * The logical plan should consist of Project, Filter and DataSource nodes only.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
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
  private List<Struct> keys;

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
    final boolean windowed = persistentQueryMetadata.getResultTopic().getKeyFormat().isWindowed();
    analysis.getWhereExpression()
        .orElseThrow(() -> WhereInfo.invalidWhereClauseException("Missing WHERE clause", windowed));

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
      if (currentLogicalNode instanceof ProjectNode) {
        currentPhysicalOp = translateProjectNode((ProjectNode)currentLogicalNode);
      } else if (currentLogicalNode instanceof FilterNode) {
        currentPhysicalOp = translateFilterNode((FilterNode)currentLogicalNode);
      } else if (currentLogicalNode instanceof DataSourceNode) {
        currentPhysicalOp = translateDataSourceNode(
            (DataSourceNode) currentLogicalNode, persistentQueryMetadata);
        dataSourceOperator = (DataSourceOperator)currentPhysicalOp;
      } else {
        throw new KsqlException("Error in translating logical to physical plan for pull queries: "
                                    + "unrecognized logical node.");
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
        ((ProjectOperator)rootPhysicalOp).getOutputSchema(),
        queryId,
        keys,
        mat,
        dataSourceOperator);
  }

  private ProjectOperator translateProjectNode(final ProjectNode logicalNode) {
    final LogicalSchema outputSchema;
    boolean isStar = false;
    if (isSelectStar(statement.getStatement().getSelect())) {
      isStar = true;
      outputSchema = buildSelectStarSchema(mat.schema(), mat.windowType().isPresent());
    } else {
      final List<SelectExpression> projection = analysis.getSelectItems().stream()
          .map(SingleColumn.class::cast)
          .map(si -> SelectExpression
              .of(si.getAlias().orElseThrow(IllegalStateException::new), si.getExpression()))
          .collect(Collectors.toList());

      outputSchema = selectOutputSchema(projection, mat.windowType());
    }
    final ProcessingLogger logger = processingLogContext
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                QueryType.PULL_QUERY, contextStacker.push("PROJECT").getQueryContext())
        );

    final boolean noSystemColumns = analysis.getSelectColumnNames().stream()
        .noneMatch(SystemColumns::isSystemColumn);
    final boolean noKeyColumns = analysis.getSelectColumnNames().stream()
        .noneMatch(mat.schema()::isKeyColumn);

    return new ProjectOperator(
      config,
      metaStore,
      logger,
      mat,
      logicalNode,
      outputSchema,
      isStar,
      noSystemColumns,
      noKeyColumns);
  }

  private SelectOperator translateFilterNode(final FilterNode logicalNode) {
    whereInfo = WhereInfo.extractWhereInfo(analysis, persistentQueryMetadata);
    return new SelectOperator(logicalNode);
  }

  private AbstractPhysicalOperator translateDataSourceNode(
      final DataSourceNode logicalNode,
      final PersistentQueryMetadata persistentQueryMetadata
  ) {
    if (whereInfo == null) {
      throw new KsqlException("Pull queries must have a WHERE clause");
    }
    keys = whereInfo.getKeysBound().stream()
        .map(keyBound -> asKeyStruct(keyBound, persistentQueryMetadata.getPhysicalSchema()))
        .collect(ImmutableList.toImmutableList());

    if (!whereInfo.isWindowed()) {
      return new KeyedTableLookupOperator(mat, logicalNode);
    } else {
      return new KeyedWindowedTableLookupOperator(
          mat, logicalNode, whereInfo.getWindowBounds().get());
    }
  }

  private Struct asKeyStruct(final Object keyValue, final PhysicalSchema physicalSchema) {
    final ConnectSchema keySchema = ConnectSchemas
        .columnsToConnectSchema(physicalSchema.keySchema().columns());

    final Field keyField = Iterables.getOnlyElement(keySchema.fields());

    final Struct key = new Struct(keySchema);
    key.put(keyField, keyValue);
    return key;
  }

  private LogicalSchema selectOutputSchema(
      final List<SelectExpression> selectExpressions,
      final Optional<WindowType> windowType
  ) {
    final Builder schemaBuilder = LogicalSchema.builder();

    // Copy meta & key columns into the value schema as SelectValueMapper expects it:
    final LogicalSchema schema = mat.schema()
        .withPseudoAndKeyColsInValue(windowType.isPresent());

    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, metaStore);

    for (final SelectExpression select : selectExpressions) {
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());

      if (mat.schema().isKeyColumn(select.getAlias())
          || select.getAlias().equals(SystemColumns.WINDOWSTART_NAME)
          || select.getAlias().equals(SystemColumns.WINDOWEND_NAME)
      ) {
        schemaBuilder.keyColumn(select.getAlias(), type);
      } else {
        schemaBuilder.valueColumn(select.getAlias(), type);
      }
    }
    return schemaBuilder.build();
  }

  private boolean isSelectStar(final Select select) {
    final boolean someStars = select.getSelectItems().stream()
        .anyMatch(s -> s instanceof AllColumns);

    if (someStars && select.getSelectItems().size() != 1) {
      throw new KsqlException("Pull queries only support wildcards in the projects "
                                  + "if they are the only expression");
    }

    return someStars;
  }

  private QueryId uniqueQueryId() {
    return new QueryId("query_" + System.currentTimeMillis());
  }

  private LogicalSchema buildSelectStarSchema(
      final LogicalSchema schema,
      final boolean windowed
  ) {
    final Builder builder = LogicalSchema.builder()
        .keyColumns(schema.key());

    if (windowed) {
      builder.keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT);
      builder.keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT);
    }

    return builder
        .valueColumns(schema.value())
        .build();
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
