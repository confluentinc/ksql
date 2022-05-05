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

package io.confluent.ksql.rest.server;

import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.ComposableStatsCalculator;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ProjectNode.Locality;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.ParameterUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.TranslateExpressions;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionBuilder;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    final SqlParser sqlParser = new SqlParser();
    final Statement statement = sqlParser.createStatement(
        "SELECT * FROM Pantalones JOIN Abrigos A ON Pantalones.ID = A.ID",
        ParsingOptions.builder().build()
    );

    System.out.println(statement);

    final WarningCollector warningCollector = WarningCollector.NOOP;
    final QueryPreparer queryPreparer = new QueryPreparer(sqlParser);
    final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    final String catalogName = "catalog";
    final String schemaName = "schema";

    final Table pantalonesTable = new Table(
        "pantalones",
        schemaName,
        ImmutableList.of(
            ColumnMetadata
                .builder()
                .setName("id")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build()
        )
    );
    final Table abrigosTable = new Table(
        "abrigos",
        schemaName,
        ImmutableList.of(
            ColumnMetadata
                .builder()
                .setName("id")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build()
        )
    );

    final CatalogManager catalogManager = new CatalogManager();
    final ConnectorId connectorId = new ConnectorId(catalogName);
    final Connector connector = new ProtoConnector(ImmutableList.of(pantalonesTable, abrigosTable));
    final Catalog catalog = new Catalog(
        catalogName,
        connectorId,
        connector,
        createInformationSchemaConnectorId(connectorId),
        connector,
        createSystemTablesConnectorId(connectorId),
        connector);
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

          final Metadata metadata = MetadataManager.createTestMetadataManager(
              transactionManager,
              new FeaturesConfig()
          );

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

          // Hack. The goal here was to avoid running any optimizations,
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
          );

          final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

          final LogicalPlanner logicalPlanner = new LogicalPlanner(
              false,
              session,
              optimizers,
              new PlanChecker(new FeaturesConfig()),
              idAllocator,
              metadata,
              sqlParser,
              statsCalculator,
              costCalculator,
              warningCollector
          );

          final Plan logicalPlan = logicalPlanner.plan(analysis);

          System.out.println(logicalPlan);

          System.out.println(
              PlanPrinter.textLogicalPlan(
                  logicalPlan.getRoot(),
                  logicalPlan.getTypes(),
                  FunctionAndTypeManager.createTestFunctionAndTypeManager(),
                  logicalPlan.getStatsAndCosts(),
                  transactionSession,
                  0
              )
          );

        });
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
      this.connectorTableHandle = new ConnectorTableHandle() {
      };
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
        public List<ConnectorTableLayoutResult> getTableLayouts(final ConnectorSession session,
            final ConnectorTableHandle table, final Constraint<ColumnHandle> constraint,
            final Optional<Set<ColumnHandle>> desiredColumns) {
          return null;
        }

        @Override
        public ConnectorTableLayout getTableLayout(final ConnectorSession session,
            final ConnectorTableLayoutHandle handle) {
          return null;
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
   * Hack. Presto's logical planner requires all projection nodes to have a
   * locality specified. I'm not sure what that means, but I don't want to worry
   * about it right now.
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
