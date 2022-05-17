/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.ComposableStatsCalculator;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ProjectNode.Locality;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.ParameterUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionBuilder;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.slice.Slice;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.structured.SchemaKSourceFactory;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.test.model.PathLocation;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.util.KsqlConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.jetbrains.annotations.NotNull;
import org.weakref.jmx.MBeanExporter;

@SuppressWarnings({
    "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:MethodLength",
    "checkstyle:HideUtilityClassConstructor"
})
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP",
    justification = "POC"
)
public class PrestoPlanner {

  public PrestoPlanner() {
  }

  public static void main(final String[] args) {
    final LogicalSchema.Builder pantalonesKsqlSchema = LogicalSchema.builder();
    pantalonesKsqlSchema.keyColumn(ColumnName.of("id"), SqlPrimitiveType.of(SqlBaseType.STRING));
    pantalonesKsqlSchema.valueColumn(ColumnName.of("size"),
        SqlPrimitiveType.of(SqlBaseType.STRING));
    pantalonesKsqlSchema.valueColumn(ColumnName.of("waist"),
        SqlPrimitiveType.of(SqlBaseType.INTEGER));
    final KsqlTable<?> pantalonesKsqlTable = new KsqlTable<>(
        "is this the table definition?",
        SourceName.of("pantalones"),
        pantalonesKsqlSchema.build(),
        Optional.empty(),
        false,
        new KsqlTopic(
            "pantalones-topic",
            KeyFormat.nonWindowed(FormatInfo.of("JSON"), SerdeFeatures.of()),
            ValueFormat.of(FormatInfo.of("JSON"), SerdeFeatures.of())
        ),
        true
    );
    final ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables = ImmutableMap.of(
        new SchemaTableName("schema", "pantalones"), pantalonesKsqlTable
    );

    final String schemaName = "schema";
    final Table pantalonesPrestoTable = new Table(
        "pantalones",
        schemaName,
        ImmutableList.of(
            ColumnMetadata
                .builder()
                .setName("id")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build(),
            ColumnMetadata
                .builder()
                .setName("size")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build(),
            ColumnMetadata
                .builder()
                .setName("waist")
                .setType(IntegerType.INTEGER)
                .setNullable(false)
                .build()
        )
    );
    final Connector connector = new ProtoConnector(
        ImmutableList.of(
            pantalonesPrestoTable, new Table(
                "abrigos",
                schemaName,
                ImmutableList.of(
                    ColumnMetadata
                        .builder()
                        .setName("id")
                        .setType(VarcharType.createUnboundedVarcharType())
                        .setNullable(false)
                        .build(),
                    ColumnMetadata
                        .builder()
                        .setName("sleeve")
                        .setType(IntegerType.INTEGER)
                        .setNullable(false)
                        .build()
                )
            )
        )
    );

    planKsql();

    logicalPlan(
        "SELECT * FROM Pantalones JOIN Abrigos A ON Pantalones.ID = A.ID",
        schemaName,
        connector
    );

    logicalPlan(
        "SELECT Pantalones.ID, Waist, Size, Sleeve FROM Pantalones JOIN Abrigos A ON Pantalones.ID = A.ID",
        schemaName,
        connector
    );

    logicalPlan(
        "SELECT * FROM Pantalones JOIN Abrigos A ON Pantalones.ID > A.ID",
        schemaName,
        connector
    );

    logicalPlan(
        "CREATE TABLE asdf AS SELECT * FROM Pantalones",
        schemaName,
        connector
    );

    logicalPlan(
        "SELECT ID, CONCAT(ID, '+'), Waist * 2 FROM Pantalones",
        schemaName,
        connector
    );

    logicalPlan(
        "(SELECT ID, Waist AS Num FROM Pantalones) UNION (SELECT ID, Sleeve as Num FROM Abrigos)",
        schemaName,
        connector
    );

    logicalPlan(
        "SELECT ID1, Waist, Size FROM (SELECT ID AS ID1, Waist FROM Pantalones) JOIN (SELECT ID AS ID2, Size FROM Pantalones) ON ID1 = ID2",
        schemaName,
        connector
    );
  }

  @NotNull
  public static PlanBuildContext getPlanBuildContext() {
    return PlanBuildContext.of(
        new KsqlConfig(Collections.emptyMap()),
        new DefaultServiceContext(
            new DefaultKafkaClientSupplier(),
            () -> null,
            () -> null,
            () -> null,
            () -> null
        ),
        new InternalFunctionRegistry(),
        Optional.empty()
    );
  }

  public static Topology getTopology(final PlanBuildContext planBuildContext,
      final SchemaKStream<?> dataOutputNodeStream) {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final RuntimeBuildContext buildContext = RuntimeBuildContext.of(
        streamsBuilder,
        new KsqlConfig(Collections.emptyMap()),
        planBuildContext.getServiceContext(),
        ProcessingLogContext.create(),
        planBuildContext.getFunctionRegistry(),
        "asfd",
        new QueryId("asfd")
    );
    final KSPlanBuilder ksPlanBuilder = new KSPlanBuilder(buildContext);
    final ExecutionStep<?> executionStep = dataOutputNodeStream.getSourceStep();
    executionStep.build(ksPlanBuilder);
    final Topology topology = streamsBuilder.build();
    return topology;
  }

  public static SchemaKStream<?> physicalPlan(
      final Plan prestoPlan,
      final ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables,
      final PlanBuildContext planBuildContext) {

    return physicalPlan(ksqlTables, planBuildContext, prestoPlan.getRoot());
  }

  private static SchemaKStream<?> physicalPlan(
      final ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables,
      final PlanBuildContext planBuildContext,
      final PlanNode node) {
    if (node instanceof TableScanNode) {
      final TableScanNode tableScanNode = (TableScanNode) node;

      // DataSourceNode
      final TableHandle table = tableScanNode.getTable();
      final MyConnectorTableHandle connectorHandle = (MyConnectorTableHandle) table.getConnectorHandle();

      final SchemaTableName schemaTableName = connectorHandle.schemaTableName();
      final KsqlTable<?> dataSource = ksqlTables.get(schemaTableName);

      final SchemaKStream<?> dataSourceStream = SchemaKSourceFactory.buildSource(
          planBuildContext,
          dataSource,
          planBuildContext.buildNodeContext(schemaTableName.toString())
              .push("Source")
      );
      return dataSourceStream;
    } else if (node instanceof OutputNode) {
      final OutputNode outputNode = (OutputNode) node;
      final PlanNode source = outputNode.getSource();
      final SchemaKStream<?> dataSourceStream = physicalPlan(ksqlTables, planBuildContext, source);

      // ProjectNode
      final Builder<ColumnName> columnNames = ImmutableList.builder();
      for (final String columnName : outputNode.getColumnNames()) {
        columnNames.add(ColumnName.of(columnName));
      }
      final Builder<SelectExpression> selectExpressions = ImmutableList.builder();
      for (final VariableReferenceExpression outputVariable : outputNode.getOutputVariables()) {
//      prestoPlan.get
        final ColumnName columnName = ColumnName.of(outputVariable.getName());
        final SelectExpression selectExpression = SelectExpression.of(
            columnName,
            new UnqualifiedColumnReferenceExp(
                columnName
            )
        );
        selectExpressions.add(selectExpression);
      }
      final SchemaKStream<?> projectNodeStream = dataSourceStream.select(
          columnNames.build(),
          selectExpressions.build(),
          planBuildContext.buildNodeContext("Output"),
          planBuildContext,
          FormatInfo.of("JSON")
      );

      // Data Output Node
      final SchemaKStream<?> dataOutputNodeStream = projectNodeStream.into(
          new KsqlTopic(
              "result",
              KeyFormat.nonWindowed(FormatInfo.of("JSON"), SerdeFeatures.of()),
              ValueFormat.of(FormatInfo.of("JSON"), SerdeFeatures.of())
          ),
          planBuildContext.buildNodeContext(outputNode.toString() + ":out"),
          Optional.empty()
      );
      return dataOutputNodeStream;
    } else if (node instanceof FilterNode) {
      final FilterNode filterNode = (FilterNode) node;
      final PlanNode source = filterNode.getSource();
      final SchemaKStream<?> dataSourceStream = physicalPlan(ksqlTables, planBuildContext, source);
      final RowExpression predicate = filterNode.getPredicate();
      final Expression expression = getExpression(predicate);
      final PlanNodeId id = filterNode.getId();
      final String context = id.toString();
      final SchemaKStream<?> filterNodeOutputStream = dataSourceStream.filter(
          expression,
          planBuildContext.buildNodeContext(context)
      );
      return filterNodeOutputStream;
    }
    throw new IllegalArgumentException();
  }

  private static Expression getExpression(final RowExpression predicate) {
    if (predicate instanceof CallExpression) {
      final CallExpression callExpression = (CallExpression) predicate;
      final List<RowExpression> arguments = callExpression.getArguments();
      if (arguments.size() == 2) {
        // boolean expressions implemented here.
        final VariableReferenceExpression leftPresto = (VariableReferenceExpression) arguments
            .get(0);
        final UnqualifiedColumnReferenceExp left =
            new UnqualifiedColumnReferenceExp(
                new ColumnName(leftPresto.getName())
            );
        final ConstantExpression rightPresto =
            (ConstantExpression) arguments.get(1);
        final Literal rightKsql = getLiteralFromConstantExpression(rightPresto);

        if (callExpression.getDisplayName().equals("EQUAL")) {
          return new ComparisonExpression(
              Type.EQUAL,
              left,
              rightKsql
          );
        } else if (callExpression.getDisplayName().equals("GREATER_THAN")) {
          return new ComparisonExpression(
              Type.GREATER_THAN,
              left,
              rightKsql
          );
        } else {
          throw new IllegalArgumentException();
        }
      } else {
        throw new IllegalArgumentException();
      }
    } else if (predicate instanceof SpecialFormExpression) {
      final SpecialFormExpression specialFormExpression = (SpecialFormExpression) predicate;
      final Form form = specialFormExpression.getForm();
      if (form == Form.IN) {
        final List<RowExpression> arguments = specialFormExpression.getArguments();
        final VariableReferenceExpression variableReferenceExpression =
            (VariableReferenceExpression) arguments.get(0);
        final UnqualifiedColumnReferenceExp inColumn = new UnqualifiedColumnReferenceExp(
            new ColumnName(variableReferenceExpression.getName())
        );
        final Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 1; i < arguments.size(); i++) {
          final Literal stringLiteral = getLiteralFromConstantExpression(
              (ConstantExpression) arguments.get(i)
          );
          expressions.add(stringLiteral);
        }
        final InListExpression inListExpression = new InListExpression(expressions.build());
        return new InPredicate(inColumn, inListExpression);
      } else {
        throw new IllegalArgumentException();
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  @NotNull
  private static Literal getLiteralFromConstantExpression(final ConstantExpression rightPresto) {
    if (rightPresto.getType() instanceof VarcharType) {
      final Slice value = (Slice) rightPresto.getValue();
      final byte[] bytes = value.getBytes();
      final String string = new String(bytes, StandardCharsets.UTF_8);
      return new StringLiteral(string);
    } else if (rightPresto.getType() instanceof IntegerType) {
      final Long value = (Long) rightPresto.getValue();
      return new LongLiteral(value);
    } else {
      throw new IllegalArgumentException();
    }
  }

  private static void planKsql() {
    try {
      final TestExecutor testExecutor = TestExecutor.create(false, Optional.empty());
      final TestCase testCase = new TestCase(
          new PathLocation(Files.createTempFile("", "")),
          Files.createTempFile("", ""),
          "asdf",
          VersionBounds.allVersions(),
          Collections.emptyMap(),
          ImmutableList.of(new Topic("asdf", Optional.empty(), Optional.empty())),
          Collections.emptyList(),
          Collections.emptyList(),
          ImmutableList.of(
              "CREATE TABLE Pantalones (ID STRING PRIMARY KEY, Waist INT, PantSize STRING) WITH (kafka_topic='asdf', value_format='JSON');",
              "CREATE TABLE Out AS SELECT * FROM Pantalones WHERE ID IN ('5', 'XYZ');"
          ),
          Optional.empty(),
          PostConditions.NONE
      );
      testExecutor.buildAndExecuteQuery(testCase, TestExecutionListener.noOp());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static SchemaKStream<?> buildStream(
      final PlanBuildContext planBuildContext,
      final Plan prestoPlan) {
    final PlanNode prestoPlanNode = prestoPlan.getRoot();
    return buildStream(planBuildContext, prestoPlanNode);
  }

  private static SchemaKStream<?> buildStream(
      final PlanBuildContext planBuildContext,
      final PlanNode prestoPlanNode) {
    if (prestoPlanNode instanceof OutputNode) {
      final OutputNode outputNode = (OutputNode) prestoPlanNode;
      final PlanNode prestoSourceNode = outputNode.getSource();
      return null;
    } else {
      throw new IllegalArgumentException(prestoPlanNode.toString());
    }
  }

  /*private static LogicalPlanNode toKsql(final Plan logicalPlan) {
    final PlanNode root = logicalPlan.getRoot();
    final io.confluent.ksql.planner.plan.OutputNode outputNode =
        (io.confluent.ksql.planner.plan.OutputNode) toKsql(root);
    final ImmutableSet<SourceName> sourceNames = ImmutableSet.of();
    final LogicalPlanNode logicalPlan1 =
        new LogicalPlanNode("asdf",Optional.of(outputNode));
    return logicalPlan1;
  }

  private static io.confluent.ksql.planner.plan.PlanNode toKsql(final PlanNode node) {
    if (node instanceof OutputNode) {
      final OutputNode outputNode = (OutputNode) node;
      final PlanNode source = outputNode.getSource();
      final io.confluent.ksql.planner.plan.PlanNode sourceKsql = toKsql(source);
      final io.confluent.ksql.planner.plan.PlanNode project = new FinalProjectNode(
          new PlanNodeId("Project"),
          sourceKsql,
          outputNode.getColumnNames()
      );
      return new KsqlBareOutputNode(
          new PlanNodeId("Output"),
          project,
          project.getSchema(),
          OptionalInt.empty(),
          Optional.empty(),
          Optional.empty()
      );
    } else {
      throw new IllegalArgumentException(node.toString());
    }
  }*/

  public static Plan logicalPlan(
      final String sql,
      final String schemaName,
      final Connector connector) {
    System.out.println(sql);

    final SqlParser sqlParser = new SqlParser();
    final Statement statement = sqlParser.createStatement(
        sql,
        ParsingOptions.builder().build()
    );

    System.out.println(statement);

    final WarningCollector warningCollector = WarningCollector.NOOP;
    final QueryPreparer queryPreparer = new QueryPreparer(sqlParser);
    final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    final String catalogName = "catalog";

    final CatalogManager catalogManager = new CatalogManager();
    final ConnectorId connectorId = new ConnectorId(catalogName);
    final ConnectorId systemConnectorId = new ConnectorId("systemCatalog");
    final Connector systemConnector = new ProtoConnector(ImmutableList.of());
    final Catalog catalog = new Catalog(
        catalogName,
        connectorId,
        connector,
        createInformationSchemaConnectorId(connectorId),
        connector,
        createSystemTablesConnectorId(systemConnectorId),
        systemConnector);
    catalogManager.registerCatalog(catalog);

    final Session session = Session.builder(new SessionPropertyManager())
        .setQueryId(queryIdGenerator.createNextQueryId())
        .setIdentity(new Identity("user", Optional.empty()))
        .setCatalog(catalogName)
        .setSchema(schemaName)
        .build();

    final AccessControl accessControl = new AllowAllAccessControl();
    final TransactionManager transactionManager =
        InMemoryTransactionManager.createTestTransactionManager(
            catalogManager
        );

    final AtomicReference<Plan> logicalPlan = new AtomicReference<>();
    TransactionBuilder.transaction(transactionManager, accessControl)
        .singleStatement()
        .execute(session, transactionSession -> {

          final Optional<TransactionId> transactionId = transactionSession.getTransactionId();
          System.out.println(transactionId);

          final PreparedQuery preparedQuery = queryPreparer.prepareQuery(
              transactionSession,
              statement,
              warningCollector
          );
          System.out.println(preparedQuery);

          BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
          final TablePropertyManager tablePropertyManager = new TablePropertyManager();
          tablePropertyManager.addProperties(connectorId, ImmutableList.of());
          final MetadataManager metadata = new MetadataManager(
              new FunctionAndTypeManager(transactionManager, blockEncodingManager,
                  new FeaturesConfig(), new HandleResolver(), ImmutableSet.of()),
              blockEncodingManager,
              new SessionPropertyManager(),
              new SchemaPropertyManager(),
              tablePropertyManager,
              new ColumnPropertyManager(),
              new AnalyzePropertyManager(),
              transactionManager);

          final Analyzer analyzer = new Analyzer(
              transactionSession,
              metadata,
              sqlParser,
              accessControl,
              Optional.empty(),
              preparedQuery.getParameters(),
              ParameterUtils.parameterExtractor(preparedQuery.getStatement(),
                  preparedQuery.getParameters()),
              warningCollector
          );

          final Analysis analysis = analyzer.analyze(
              preparedQuery.getStatement()
          );

          System.out.println(analysis);

          final StatsCalculator statsCalculator =
              new ComposableStatsCalculator(Collections.emptyList());
          final CostCalculator costCalculator = new CostCalculatorUsingExchanges(
              new TaskCountEstimator(() -> 42));
          final RuleStatsRecorder ruleStats = new RuleStatsRecorder();

          final boolean forceSingleNode = true;
          final MBeanExporter exporter = null;
          final SplitManager splitManager = new SplitManager(metadata, new QueryManagerConfig(),
              new NodeSchedulerConfig());
          final ConnectorPlanOptimizerManager connectorPlanOptimizerManager = new ConnectorPlanOptimizerManager();
          final PageSourceManager pageSourceManager = new PageSourceManager();
          final CostComparator costComparator = new CostComparator(1.0, 1.0, 1.0);
          final TaskCountEstimator taskCountEstimator = null;
          final PartitioningProviderManager partitioningProviderManager = null;
          PlanOptimizers po = new PlanOptimizers(
              metadata,
              sqlParser,
              forceSingleNode,
              exporter,
              splitManager,
              connectorPlanOptimizerManager,
              pageSourceManager,
              statsCalculator,
              costCalculator,
              costCalculator,
              costComparator,
              taskCountEstimator,
              partitioningProviderManager
          );

          final List<PlanOptimizer> planningTimeOptimizers = po.getPlanningTimeOptimizers();

/*          // Hack. The goal here was to avoid running any optimizations,
          // but these two are required rewrite rules to produce a valid plan.
          final List<PlanOptimizer> optimizers = ImmutableList.of(
              new IterativeOptimizer(
                  ruleStats,
                  statsCalculator,
                  costCalculator,
                  new TranslateExpressions(metadata, sqlParser).rules()
              ),
              new IterativeOptimizer(
                  ruleStats,
                  statsCalculator,
                  costCalculator,
                  Collections.singleton(new ProjectLocalityRewrite())
              )
          );*/

          final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

          final LogicalPlanner logicalPlanner = new LogicalPlanner(
              false,
              transactionSession,
              planningTimeOptimizers,
              new PlanChecker(new FeaturesConfig()),
              idAllocator,
              metadata,
              sqlParser,
              statsCalculator,
              costCalculator,
              warningCollector
          );

          final Plan plan = logicalPlanner.plan(analysis);

          System.out.println(plan);

          System.out.println(
              PlanPrinter.textLogicalPlan(
                  plan.getRoot(),
                  plan.getTypes(),
                  FunctionAndTypeManager.createTestFunctionAndTypeManager(),
                  plan.getStatsAndCosts(),
                  transactionSession,
                  0
              )
          );

          logicalPlan.set(plan);
        });

    return logicalPlan.get();
  }


  public static class Table {


    private final String tableName;
    private final String schemaName;
    private final SchemaTableName schemaTableName;
    private final ConnectorTableHandle connectorTableHandle;
    private final ConnectorTableMetadata connectorTableMetadata;
    private final Map<String, ColumnHandle> columnHandles;

    public Table(
        final String tableName,
        final String schemaName,
        final List<ColumnMetadata> columnMetadata) {

      this.tableName = tableName;
      this.schemaName = schemaName;
      this.schemaTableName = new SchemaTableName(schemaName, tableName);
      this.connectorTableHandle = new MyConnectorTableHandle(schemaTableName);
      this.connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, columnMetadata);
      this.columnHandles = columnMetadata.stream().map(ColumnMetadata::getName).collect(
          Collectors.toMap(name -> name, name -> new ColumnHandle() {
          })
      );
    }

    public SchemaTableName getSchemaTableName() {
      return schemaTableName;
    }

    public ConnectorTableHandle getTableHandle() {
      return connectorTableHandle;
    }

    public ConnectorTableMetadata getConnectorTableMetadata() {
      return this.connectorTableMetadata;
    }

    public ConnectorTableHandle getConnectorTableHandle() {
      return this.connectorTableHandle;
    }

    public Map<String, ColumnHandle> getColumnHandles() {
      return this.columnHandles;
    }


  }

  public static class MyConnectorTableHandle implements ConnectorTableHandle {

    private final SchemaTableName schemaTableName;

    public MyConnectorTableHandle(final SchemaTableName schemaTableName) {
      this.schemaTableName = schemaTableName;
    }

    public SchemaTableName schemaTableName() {
      return schemaTableName;
    }

    @Override
    public String toString() {
      return super.toString() + ":" + schemaTableName;
    }
  }

  public static class ProtoConnector implements Connector {

    private List<String> schemaNames;
    private final Map<SchemaTableName, ConnectorTableHandle> tables;
    private final Map<ConnectorTableHandle, ConnectorTableMetadata> tableMetadata;
    private final Map<ConnectorTableHandle, Map<String, ColumnHandle>> tableColumns;

    public ProtoConnector(final ImmutableList<Table> tables) {
      this.schemaNames = tables.stream().map(Table::getSchemaTableName)
          .map(SchemaTableName::getSchemaName).collect(Collectors.toList());
      this.tables = tables.stream()
          .collect(Collectors.toMap(Table::getSchemaTableName, Table::getTableHandle));
      this.tableMetadata = tables.stream().collect(
          Collectors.toMap(Table::getConnectorTableHandle, Table::getConnectorTableMetadata));
      this.tableColumns = tables.stream()
          .collect(Collectors.toMap(Table::getConnectorTableHandle, Table::getColumnHandles));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(final IsolationLevel isolationLevel,
        final boolean readOnly) {
      return new ConnectorTransactionHandle() {
      };
    }

    @Override
    public ConnectorMetadata getMetadata(final ConnectorTransactionHandle transactionHandle) {
      return new ConnectorMetadata() {

        @Override
        public List<String> listSchemaNames(final ConnectorSession session) {
          return schemaNames;
        }

        @Override
        public ConnectorTableHandle getTableHandle(
            final ConnectorSession session,
            final SchemaTableName tableName) {
          return tables.get(tableName);
        }

        @Override
        public List<ConnectorTableLayoutResult> getTableLayouts(
            final ConnectorSession session,
            final ConnectorTableHandle table,
            final Constraint<ColumnHandle> constraint,
            final Optional<Set<ColumnHandle>> desiredColumns) {
          final ConnectorTableLayoutHandle handle = new ConnectorTableLayoutHandle() {
            @Override
            public String toString() {
              return super.toString() + ":" + table.toString();
            }
          };
          final ConnectorTableLayout layout = new ConnectorTableLayout(handle);
          final ConnectorTableLayoutResult connectorTableLayoutResult =
              new ConnectorTableLayoutResult(
                  layout,
                  constraint.getSummary()
              );
          return ImmutableList.of(connectorTableLayoutResult);
        }

        @Override
        public ConnectorTableLayout getTableLayout(
            final ConnectorSession session,
            final ConnectorTableLayoutHandle handle) {
          return new ConnectorTableLayout(handle);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
            final ConnectorTableHandle table) {
          return tableMetadata.get(table);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
            final ConnectorTableHandle tableHandle) {
          return tableColumns.get(tableHandle);
        }

        @Override
        public ColumnMetadata getColumnMetadata(final ConnectorSession session,
            final ConnectorTableHandle tableHandle, final ColumnHandle columnHandle) {
          return null;
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            final ConnectorSession session, final SchemaTablePrefix prefix) {
          return null;
        }
      };
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
      return null;
    }
  }

  /**
   * Hack. Presto's logical planner requires all projection nodes to have a locality specified. I'm
   * not sure what that means, but I don't want to worry about it right now.
   *
   * <p>In the full set of optimizers that Presto runs, it winds up writing that locality,
   * but I didn't want to pull in a bunch of optimizers we don't understand.
   */
  private static class ProjectLocalityRewrite implements Rule<ProjectNode> {

    @Override
    public Pattern<ProjectNode> getPattern() {
      return Pattern.typeOf(ProjectNode.class);
    }

    @Override
    public Result apply(final ProjectNode node, final Captures captures,
        final Context context) {
      switch (node.getLocality()) {
        case LOCAL:
        case REMOTE:
          return Result.empty();
        case UNKNOWN:
        default:
          return Result.ofPlanNode(
              new ProjectNode(
                  node.getSourceLocation(),
                  node.getId(),
                  node.getSource(),
                  node.getAssignments(),
                  Locality.LOCAL
              )
          );
      }
    }
  }
}
