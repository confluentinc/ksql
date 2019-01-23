/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static java.util.regex.Pattern.compile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StatementWithSchema;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.connect.data.Schema;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@Path("/ksql")
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class KsqlResource {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Map<Class<? extends Statement>, Validator<Statement>> CUSTOM_VALIDATORS =
      ImmutableMap.<Class<? extends Statement>, Validator<Statement>>builder()
          .put(Query.class,
              castValidator(KsqlResource::validateQueryEndpointStatements, Query.class))
          .put(PrintTopic.class,
              castValidator(KsqlResource::validateQueryEndpointStatements, PrintTopic.class))
          .put(SetProperty.class,
              castValidator(KsqlResource::validateSetPropertyStatement, SetProperty.class))
          .put(UnsetProperty.class,
              castValidator(KsqlResource::validateUnsetPropertyStatement, UnsetProperty.class))
          .put(ShowColumns.class,
              castValidator(KsqlResource::showColumns, ShowColumns.class))
          .put(Explain.class,
              castValidator(KsqlResource::explain, Explain.class))
          .put(DescribeFunction.class,
              castValidator(KsqlResource::describeFunction, DescribeFunction.class))
          .build();

  private static final Map<Class<? extends Statement>, Handler<Statement>> CUSTOM_EXECUTORS =
      ImmutableMap.<Class<? extends Statement>, Handler<Statement>>builder()
          .put(ListTopics.class,
              castExecutor(KsqlResource::listTopics, ListTopics.class))
          .put(ListRegisteredTopics.class,
              castExecutor(KsqlResource::listRegisteredTopics, ListRegisteredTopics.class))
          .put(ListStreams.class,
              castExecutor(KsqlResource::listStreams, ListStreams.class))
          .put(ListTables.class,
              castExecutor(KsqlResource::listTables, ListTables.class))
          .put(ListFunctions.class,
              castExecutor(KsqlResource::listFunctions, ListFunctions.class))
          .put(ListQueries.class,
              castExecutor(KsqlResource::listQueries, ListQueries.class))
          .put(ShowColumns.class,
              castExecutor(KsqlResource::showColumns, ShowColumns.class))
          .put(ListProperties.class,
              castExecutor(KsqlResource::listProperties, ListProperties.class))
          .put(Explain.class,
              castExecutor(KsqlResource::explain, Explain.class))
          .put(DescribeFunction.class,
              castExecutor(KsqlResource::describeFunction, DescribeFunction.class))
          .put(SetProperty.class,
              castExecutor(KsqlResource::setProperty, SetProperty.class))
          .put(UnsetProperty.class,
              castExecutor(KsqlResource::unsetProperty, UnsetProperty.class))
          .put(RunScript.class,
              castExecutor(KsqlResource::distributeStatement, RunScript.class))
          .put(TerminateQuery.class,
              castExecutor(KsqlResource::distributeStatement, TerminateQuery.class))
          .build();

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final ActivenessRegistrar activenessRegistrar;

  public KsqlResource(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final ActivenessRegistrar activenessRegistrar
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar cannot be null.");
  }

  @POST
  @Path("/terminate")
  public Response terminateCluster(final ClusterTerminateRequest request) {
    final KsqlEntityList result = new KsqlEntityList();

    ensureValidPatterns(request.getDeleteTopicList());

    try {
      result.add(distributeStatement(
          new PreparedStatement<>(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT,
              new TerminateCluster()),
          request.getStreamsProperties()
      ));

      return Response.ok(result).build();
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(
          e,
          TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT,
          result);
    }
  }

  @POST
  public Response handleKsqlStatements(final KsqlRequest request) {
    if (!ksqlEngine.isAcceptingStatements()) {
      return Errors.serverErrorForStatement(
          new KsqlException("The cluster has been terminated. No new request will be accepted."),
          request.getKsql(),
          new KsqlEntityList()
      );
    }
    activenessRegistrar.updateLastRequestTime();
    try {
      CommandStoreUtil.httpWaitForCommandSequenceNumber(
          commandQueue, request, distributedCmdResponseTimeout);

      final List<PreparedStatement<?>> statements = parseStatements(request.getKsql());

      return executeStatements(statements, request.getStreamsProperties());
    } catch (final KsqlRestException e) {
      throw e;
    } catch (final KsqlStatementException e) {
      return Errors.badStatement(e.getRawMessage(), e.getSqlStatement());
    } catch (final KsqlException e) {
      return Errors.badRequest(e);
    } catch (final Exception e) {
      return Errors.serverErrorForStatement(e, request.getKsql(), new KsqlEntityList());
    }
  }

  private List<PreparedStatement<?>> parseStatements(final String sql) {
    try {
      final List<PreparedStatement<?>> statements = ksqlEngine.parseStatements(sql);
      checkPersistentQueryCapacity(statements, sql);
      return statements;
    } catch (final KsqlStatementException e) {
      throw new KsqlRestException(Errors.badStatement(e.getCause(), e.getSqlStatement()));
    }
  }

  private void validateStatement(
      final PreparedStatement<?> statement,
      final Map<String, Object> propertyOverrides,
      final KsqlEntityList entities
  ) {
    try {
      final Validator<?> customValidator = getCustomValidator(statement);
      if (customValidator != null) {
        customValidateStatement(statement, propertyOverrides);
      } else if (KsqlEngine.isExecutableStatement(statement)) {
        validateExecutableStatement(statement, propertyOverrides);
      }

      validateCanExecute(statement, entities);
    } catch (final KsqlRestException e) {
      throw e;
    } catch (final ShouldUseQueryEndpointException e) {
      throw new KsqlRestException(Errors.queryEndpoint(e.getMessage(), entities));
    } catch (final KsqlException e) {
      throw new KsqlRestException(
          Errors.badStatement(e, statement.getStatementText(), entities));
    } catch (final Exception e) {
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, statement.getStatementText(), entities));
    }
  }

  private <T extends Statement> void customValidateStatement(
      final PreparedStatement<T> statement,
      final Map<String, Object> propertyOverrides
  ) {
    final Validator<T> validator = getCustomValidator(statement);
    if (validator != null) {
      validator.validate(this, statement, propertyOverrides);
    }
  }

  private Response executeStatements(
      final List<? extends PreparedStatement<?>> statements,
      final Map<String, Object> propertyOverrides
  ) {
    final KsqlEntityList entities = new KsqlEntityList();

    final HashMap<String, Object> requestScopedOverrides = new HashMap<>(propertyOverrides);

    for (final PreparedStatement<?> statement : statements) {
      validateStatement(statement, Collections.unmodifiableMap(requestScopedOverrides), entities);

      final KsqlEntity entity = executeStatement(statement, requestScopedOverrides, entities);
      if (entity != null) {
        entities.add(entity);
      }
    }

    return Response.ok(entities).build();
  }

  private <T extends Statement> KsqlEntity executeStatement(
      final PreparedStatement<T> statement,
      final Map<String, Object> propertyOverrides,
      final KsqlEntityList entities) {
    try {
      final Handler<T> handler = getCustomExecutor(statement);
      if (handler != null) {
        return handler.handle(this, statement, propertyOverrides);
      }

      return distributeStatement(statement, propertyOverrides);
    } catch (final KsqlRestException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlRestException(
          Errors.badStatement(e, statement.getStatementText(), entities));
    } catch (final Exception e) {
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, statement.getStatementText(), entities));
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Can not be static as used in validator map
  private void validateQueryEndpointStatements(final PreparedStatement<?> statement) {
    throw new ShouldUseQueryEndpointException(statement.getStatementText());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  private void validateSetPropertyStatement(final PreparedStatement<SetProperty> statement) {
    if (isUnknownPropertyName(statement.getStatement().getPropertyName())) {
      throw new KsqlRestException(Errors.badStatement(
          "Unknown property", statement.getStatementText()));
    }

    try {
      ksqlConfig.cloneWithPropertyOverwrite(ImmutableMap.of(
          statement.getStatement().getPropertyName(),
          statement.getStatement().getPropertyValue()));
    } catch (final Exception e) {
      throw new KsqlRestException(Errors.badStatement(e, statement.getStatementText()));
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Can not be static as used in validator map
  private void validateUnsetPropertyStatement(final PreparedStatement<UnsetProperty> statement) {
    if (isUnknownPropertyName(statement.getStatement().getPropertyName())) {
      throw new KsqlRestException(Errors.badStatement(
          "Unknown property", statement.getStatementText()));
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Can not be static as used in validator map
  private KsqlEntity setProperty(
      final PreparedStatement<SetProperty> stmt,
      final Map<String, Object> propertyOverrides
  ) {
    final SetProperty statement = stmt.getStatement();
    propertyOverrides.put(statement.getPropertyName(), statement.getPropertyValue());
    return null;
  }

  @SuppressWarnings("MethodMayBeStatic") // Can not be static as used in validator map
  private KsqlEntity unsetProperty(
      final PreparedStatement<UnsetProperty> stmt,
      final Map<String, Object> propertyOverrides
  ) {
    final UnsetProperty statement = stmt.getStatement();
    propertyOverrides.remove(statement.getPropertyName());
    return null;
  }

  private static boolean isUnknownPropertyName(final String propertyName) {
    return !new KsqlConfigResolver()
        .resolve(propertyName, false)
        .isPresent();
  }

  private KafkaTopicsList listTopics(final PreparedStatement<ListTopics> statement) {
    final KafkaTopicClient client = serviceContext.getTopicClient();
    final KafkaConsumerGroupClient kafkaConsumerGroupClient
        = new KafkaConsumerGroupClientImpl(serviceContext.getAdminClient());

    return KafkaTopicsList.build(
        statement.getStatementText(),
        ksqlEngine.getMetaStore().getAllKsqlTopics().values(),
        client.describeTopics(client.listNonInternalTopicNames()),
        ksqlConfig,
        kafkaConsumerGroupClient
    );
  }

  private KsqlTopicsList listRegisteredTopics(final PreparedStatement<ListRegisteredTopics> stmt) {
    return KsqlTopicsList.build(
        stmt.getStatementText(),
        ksqlEngine.getMetaStore().getAllKsqlTopics().values()
    );
  }

  private KsqlEntity listStreams(final PreparedStatement<ListStreams> statement) {
    final List<KsqlStream> ksqlStreams = getSpecificSources(KsqlStream.class);

    if (statement.getStatement().getShowExtended()) {
      return new SourceDescriptionList(
          statement.getStatementText(),
          ksqlStreams.stream()
              .map(s -> describeSource(s.getName(), true))
              .collect(Collectors.toList()));
    }

    return new StreamsList(
        statement.getStatementText(),
        ksqlStreams.stream()
            .map(SourceInfo.Stream::new)
            .collect(Collectors.toList()));
  }

  private KsqlEntity listTables(final PreparedStatement<ListTables> statement) {
    final List<KsqlTable> ksqlTables = getSpecificSources(KsqlTable.class);

    if (statement.getStatement().getShowExtended()) {
      return new SourceDescriptionList(
          statement.getStatementText(),
          ksqlTables.stream()
              .map(t -> describeSource(t.getName(), true))
              .collect(Collectors.toList()));
    }
    return new TablesList(
        statement.getStatementText(),
        ksqlTables.stream()
            .map(SourceInfo.Table::new)
            .collect(Collectors.toList()));
  }

  private KsqlEntity listFunctions(final PreparedStatement<ListFunctions> statement) {
    final FunctionRegistry functionRegistry = ksqlEngine.getFunctionRegistry();

    final List<SimpleFunctionInfo> all = functionRegistry.listFunctions().stream()
        .filter(factory -> !factory.isInternal())
        .map(factory -> new SimpleFunctionInfo(
            factory.getName().toUpperCase(),
            FunctionType.scalar))
        .collect(Collectors.toList());

    all.addAll(functionRegistry.listAggregateFunctions().stream()
        .filter(factory -> !factory.isInternal())
        .map(factory -> new SimpleFunctionInfo(
            factory.getName().toUpperCase(),
            FunctionType.aggregate))
        .collect(Collectors.toList()));

    return new FunctionNameList(statement.getStatementText(), all);
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private KsqlEntity listQueries(final PreparedStatement<ListQueries> statement) {
    if (statement.getStatement().getShowExtended()) {
      return new QueryDescriptionList(
          statement.getStatementText(),
          ksqlEngine.getPersistentQueries().stream()
              .map(QueryDescription::forQueryMetadata)
              .collect(Collectors.toList()));
    }

    return new Queries(
        statement.getStatementText(),
        ksqlEngine.getPersistentQueries().stream()
            .map(
                q -> new RunningQuery(
                    q.getStatementString(),
                    q.getSinkNames(),
                    new EntityQueryId(q.getQueryId())))
            .collect(Collectors.toList()));
  }

  private KsqlEntity showColumns(final PreparedStatement<ShowColumns> statement) {
    final ShowColumns showColumns = statement.getStatement();
    if (showColumns.isTopic()) {
      return describeTopic(statement.getStatementText(), showColumns.getTable().getSuffix());
    }

    return new SourceDescriptionEntity(
        statement.getStatementText(),
        describeSource(showColumns.getTable().getSuffix(), showColumns.isExtended())
    );
  }

  private PropertiesList listProperties(
      final PreparedStatement<ListProperties> statement,
      final Map<String, Object> propertyOverrides
  ) {
    final KsqlConfigResolver resolver = new KsqlConfigResolver();

    final Map<String, String> engineProperties
        = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();

    final Map<String, String> mergedProperties = ksqlConfig
        .cloneWithPropertyOverwrite(propertyOverrides)
        .getAllConfigPropsWithSecretsObfuscated();

    final List<String> overwritten = mergedProperties.entrySet()
        .stream()
        .filter(e -> !Objects.equals(engineProperties.get(e.getKey()), e.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toList());

    final List<String> defaultProps = mergedProperties.entrySet().stream()
        .filter(e -> resolver.resolve(e.getKey(), false)
            .map(resolved -> resolved.isDefaultValue(e.getValue()))
            .orElse(false))
        .map(Entry::getKey)
        .collect(Collectors.toList());

    return new PropertiesList(
        statement.getStatementText(), mergedProperties, overwritten, defaultProps);
  }

  private QueryDescriptionEntity explain(
      final PreparedStatement<Explain> statement,
      final Map<String, Object> propertyOverrides
  ) {
    final String queryId = statement.getStatement().getQueryId();

    final QueryDescription queryDescription = queryId == null
        ? explainStatement(
        statement.getStatement().getStatement(),
        statement.getStatementText().substring("EXPLAIN ".length()),
        propertyOverrides)
        : explainQuery(queryId);

    return new QueryDescriptionEntity(statement.getStatementText(), queryDescription);
  }

  private QueryDescription explainStatement(
      final Statement statement,
      final String statementText,
      final Map<String, Object> propertyOverrides
  ) {
    if (!(statement instanceof Query || statement instanceof QueryContainer)) {
      throw new KsqlException("The provided statement does not run a ksql query");
    }

    final List<QueryMetadata> metadata = ksqlEngine.tryExecute(
        ImmutableList.of(new PreparedStatement<>(statementText, statement)),
        ksqlConfig, propertyOverrides);

    return QueryDescription.forQueryMetadata(metadata.get(0));
  }

  private QueryDescription explainQuery(final String queryId) {
    final PersistentQueryMetadata metadata = ksqlEngine.getPersistentQuery(new QueryId(queryId))
        .orElseThrow(() -> new KsqlException(
            "Query with id:" + queryId + " does not exist, "
                + "use SHOW QUERIES to view the full set of queries."));

    return QueryDescription.forQueryMetadata(metadata);
  }

  private FunctionDescriptionList describeFunction(final PreparedStatement<DescribeFunction> stmt) {
    final String functionName = stmt.getStatement().getFunctionName();

    if (ksqlEngine.getFunctionRegistry().isAggregate(functionName)) {
      return describeAggregateFunction(functionName, stmt.getStatementText());
    }

    return describeNonAggregateFunction(functionName, stmt.getStatementText());
  }

  private FunctionDescriptionList describeAggregateFunction(
      final String functionName,
      final String statementText
  ) {
    final AggregateFunctionFactory aggregateFactory
        = ksqlEngine.getFunctionRegistry().getAggregateFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    aggregateFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(func.getArgTypes(), func.getReturnType(), func.getDescription())));

    return new FunctionDescriptionList(
        statementText,
        aggregateFactory.getName().toUpperCase(),
        aggregateFactory.getDescription(),
        aggregateFactory.getAuthor(),
        aggregateFactory.getVersion(),
        aggregateFactory.getPath(),
        listBuilder.build(),
        FunctionType.aggregate
    );
  }

  private FunctionDescriptionList describeNonAggregateFunction(
      final String functionName,
      final String statementText
  ) {
    final UdfFactory udfFactory = ksqlEngine.getFunctionRegistry().getUdfFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    udfFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(func.getArguments(), func.getReturnType(), func.getDescription())));

    return new FunctionDescriptionList(
        statementText,
        udfFactory.getName().toUpperCase(),
        udfFactory.getDescription(),
        udfFactory.getAuthor(),
        udfFactory.getVersion(),
        udfFactory.getPath(),
        listBuilder.build(),
        FunctionType.scalar
    );
  }

  private void validateExecutableStatement(
      final PreparedStatement<?> statement,
      final Map<String, Object> propertyOverrides
  ) {
    final PreparedStatement<?> withSchema = addInferredSchema(statement);
    ksqlEngine.tryExecute(ImmutableList.of(withSchema), ksqlConfig, propertyOverrides);
  }

  private CommandStatusEntity distributeStatement(
      final PreparedStatement<?> statement,
      final Map<String, Object> propertyOverrides
  ) {
    try {
      final PreparedStatement<?> withSchema = addInferredSchema(statement);

      final QueuedCommandStatus queuedCommandStatus = commandQueue.enqueueCommand(
          withSchema.getStatementText(),
          withSchema.getStatement(),
          ksqlConfig,
          propertyOverrides);

      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return new CommandStatusEntity(
          withSchema.getStatementText(),
          queuedCommandStatus.getCommandId(),
          commandStatus,
          queuedCommandStatus.getCommandSequenceNumber()
      );
    } catch (final Exception e) {
      throw new KsqlException(String.format(
          "Could not write the statement '%s' into the command " + "topic.",
          statement.getStatementText()), e);
    }
  }

  private TopicDescription describeTopic(
      final String statementText,
      final String name
  ) {
    final KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(name);
    if (ksqlTopic == null) {
      throw new KsqlException(String.format(
          "Could not find Topic '%s' in the Metastore",
          name
      ));
    }

    return new TopicDescription(
        statementText,
        name,
        ksqlTopic.getKafkaTopicName(),
        ksqlTopic
            .getKsqlTopicSerDe()
            .getSerDe()
            .toString(),
        null
    );
  }

  private SourceDescription describeSource(
      final String name,
      final boolean extended
  ) {
    final StructuredDataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new KsqlException(String.format(
          "Could not find STREAM/TABLE '%s' in the Metastore",
          name
      ));
    }

    return new SourceDescription(
        dataSource,
        extended,
        dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name(),
        getQueries(q -> q.getSourceNames().contains(dataSource.getName())),
        getQueries(q -> q.getSinkNames().contains(dataSource.getName())),
        serviceContext.getTopicClient()
    );
  }

  private List<RunningQuery> getQueries(final Predicate<PersistentQueryMetadata> predicate) {
    return ksqlEngine.getPersistentQueries()
        .stream()
        .filter(predicate)
        .map(q -> new RunningQuery(
            q.getStatementString(), q.getSinkNames(), new EntityQueryId(q.getQueryId())))
        .collect(Collectors.toList());
  }

  private <S extends StructuredDataSource> List<S> getSpecificSources(
      final Class<S> dataSourceClass) {
    return ksqlEngine.getMetaStore().getAllStructuredDataSources().values().stream()
        .filter(dataSourceClass::isInstance)
        .filter(structuredDataSource -> !structuredDataSource.getName().equalsIgnoreCase(
            KsqlRestApplication.getCommandsStreamName()))
        .map(dataSourceClass::cast)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private PreparedStatement<?> addInferredSchema(final PreparedStatement<?> stmt) {
    return StatementWithSchema
        .forStatement((PreparedStatement) stmt, serviceContext.getSchemaRegistryClient());
  }

  private static FunctionInfo getFunctionInfo(
      final List<Schema> argTypes,
      final Schema returnTypeSchema,
      final String description
  ) {
    final List<ArgumentInfo> args = argTypes.stream()
        .map(s -> new ArgumentInfo(s.name(), SchemaUtil.getSqlTypeName(s), s.doc()))
        .collect(Collectors.toList());

    final String returnType = SchemaUtil.getSqlTypeName(returnTypeSchema);

    return new FunctionInfo(args, returnType, description);
  }

  private void checkPersistentQueryCapacity(
      final List<? extends PreparedStatement> parsedStatements,
      final String queriesString
  ) {
    final long numQueries = parsedStatements.stream().filter(parsedStatement -> {
      final Statement statement = parsedStatement.getStatement();
      // Note: RunScript commands also have the potential to create persistent queries,
      // but we don't count those queries here (to avoid parsing those commands)
      return statement instanceof CreateAsSelect || statement instanceof InsertInto;
    }).count();

    if (QueryCapacityUtil.exceedsPersistentQueryCapacity(ksqlEngine, ksqlConfig, numQueries)) {
      QueryCapacityUtil.throwTooManyActivePersistentQueriesException(
          ksqlEngine, ksqlConfig, queriesString);
    }
  }

  private static void validateCanExecute(
      final PreparedStatement<?> statement,
      final KsqlEntityList entities
  ) {
    if (getCustomExecutor(statement) == null
        && !KsqlEngine.isExecutableStatement(statement)) {
      throw new KsqlRestException(Errors.badStatement(
          "Do not know how to execute statement", statement.getStatementText(), entities));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Statement> Validator<T> getCustomValidator(
      final PreparedStatement<T> statement
  ) {
    final Class<? extends Statement> type = statement.getStatement().getClass();
    return (Validator) CUSTOM_VALIDATORS.get(type);
  }

  @SuppressWarnings({"unchecked", "unused", "SameParameterValue"})
  private static <T extends Statement> Validator<Statement> castValidator(
      final Validator<? super T> handler,
      final Class<T> type) {
    return ((Validator<Statement>) handler);
  }

  @SuppressWarnings({"unchecked", "unused"})
  private static <T extends Statement> Validator<Statement> castValidator(
      final BiConsumer<KsqlResource, PreparedStatement<T>> validator,
      final Class<T> type) {
    return (ksqlResource, statement, propertyOverrides) ->
        ((BiConsumer) validator).accept(ksqlResource, statement);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Statement> Handler<T> getCustomExecutor(
      final PreparedStatement<T> statement
  ) {
    final Class<? extends Statement> type = statement.getStatement().getClass();
    return (Handler) CUSTOM_EXECUTORS.get(type);
  }

  @SuppressWarnings({"unchecked", "unused"})
  private static <T extends Statement> Handler<Statement> castExecutor(
      final Handler<? super T> handler,
      final Class<T> type) {
    return ((Handler<Statement>) handler);
  }

  @SuppressWarnings({"unchecked", "unused"})
  private static <T extends Statement> Handler<Statement> castExecutor(
      final BiFunction<KsqlResource, PreparedStatement<T>, ? extends KsqlEntity> handler,
      final Class<T> type) {
    return (ksqlResource, statement, propertyOverrides) ->
        (KsqlEntity) ((BiFunction) handler).apply(ksqlResource, statement);
  }

  @FunctionalInterface
  private interface Validator<T extends Statement> {

    void validate(
        KsqlResource ksqlResource,
        PreparedStatement<T> statement,
        Map<String, Object> propertyOverrides);
  }

  @FunctionalInterface
  private interface Handler<T extends Statement> {

    KsqlEntity handle(
        KsqlResource ksqlResource,
        PreparedStatement<T> statement,
        Map<String, Object> propertyOverrides);
  }

  private static final class ShouldUseQueryEndpointException extends RuntimeException {

    private ShouldUseQueryEndpointException(final String statementText) {
      super(statementText);
    }
  }

  private static void ensureValidPatterns(final List<String> deleteTopicList) {
    deleteTopicList
        .forEach(pattern -> {
          try {
            compile(pattern);
          } catch (final PatternSyntaxException patternSyntaxException) {
            throw new KsqlRestException(Errors.badRequest("Invalid pattern: " + pattern));
          }
        });
  }
}
