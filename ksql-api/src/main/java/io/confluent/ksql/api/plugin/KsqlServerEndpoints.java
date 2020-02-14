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

package io.confluent.ksql.api.plugin;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.server.PushQueryHandler;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.reactivestreams.Subscriber;

public class KsqlServerEndpoints implements Endpoints {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;
  private final ServiceContextFactory serviceContextFactory;
  private final PullQueryApiExecutor pullQueryApiExecutor;

  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension,
      final ServiceContextFactory serviceContextFactory,
      final PullQueryApiExecutor pullQueryApiExecutor) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.securityExtension = Objects.requireNonNull(securityExtension);
    this.serviceContextFactory = Objects.requireNonNull(serviceContextFactory);
    this.pullQueryApiExecutor = Objects.requireNonNull(pullQueryApiExecutor);
  }

  public QueryPublisher createQueryPublisher(
      final String sql, final JsonObject properties,
      final Context context,
      final WorkerExecutor workerExecutor) {

    // Must be run on worker as all this stuff is slow
    Utils.checkIsWorker();

    properties.put("auto.offset.reset", "earliest");

    final ServiceContext serviceContext = createServiceContext(new DummyPrincipal());
    final ConfiguredStatement<Query> statement = createStatement(sql, properties.getMap());

    if (statement.getStatement().isPullQuery()) {
      return createPullQueryPublisher(context, serviceContext, statement);
    } else {
      return createPushQueryPublisher(context, serviceContext, statement, workerExecutor);
    }
  }

  private QueryPublisher createPushQueryPublisher(final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement, final WorkerExecutor workerExecutor) {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context,
        workerExecutor);
    final QueryMetadata queryMetadata = ksqlEngine
        .executeQuery(serviceContext, statement, publisher);
    final KsqlQueryHandle queryHandle = new KsqlQueryHandle(queryMetadata,
        statement.getStatement().getLimit());
    publisher.setQueryHandle(queryHandle);
    return publisher;
  }

  private QueryPublisher createPullQueryPublisher(final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement) {
    final TableRows tableRows = pullQueryApiExecutor.execute(statement, serviceContext);
    return new PullQueryPublisher(context, tableRows, colNamesFromSchema(tableRows.getSchema()),
        colTypesFromSchema(tableRows.getSchema()));
  }

  private ConfiguredStatement<Query> createStatement(final String queryString,
      final Map<String, Object> properties) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      throw new KsqlStatementException(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()),
          queryString);
    }
    final PreparedStatement<?> ps = ksqlEngine.prepare(statements.get(0));
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      throw new KsqlStatementException("Not a query", queryString);
    }
    @SuppressWarnings("unchecked") final PreparedStatement<Query> psq =
        (PreparedStatement<Query>) ps;
    return ConfiguredStatement.of(psq, properties, ksqlConfig);
  }

  private ServiceContext createServiceContext(final Principal principal) {
    // Creates a ServiceContext using the user's credentials, so the WS query topics are
    // accessed with the user permission context (defaults to KSQL service context)

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return createServiceContext(new DefaultKafkaClientSupplier(),
          new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get);
    }

    return securityExtension.getUserContextProvider()
        .map(provider ->
            createServiceContext(
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)
            ))
        .get();
  }

  private ServiceContext createServiceContext(
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory
  ) {
    return serviceContextFactory.create(ksqlConfig,
        Optional.empty(),
        kafkaClientSupplier, srClientFactory);
  }

  private static class DummyPrincipal implements Principal {

    @Override
    public String getName() {
      return "NO_PRINCIPAL";
    }
  }

  @Override
  public InsertsStreamSubscriber createInsertsSubscriber(final String target,
      final JsonObject properties,
      final Subscriber<InsertResult> acksSubscriber, final Context context,
      final WorkerExecutor workerExecutor) {
    Utils.checkIsWorker();
    final ServiceContext serviceContext = createServiceContext(new DummyPrincipal());
    final DataSource dataSource = getDataSource(ksqlConfig, ksqlEngine.getMetaStore(),
        SourceName.of(target));
    if (dataSource.getDataSourceType() == DataSourceType.KTABLE) {
      throw new KsqlException("Cannot insert into a table");
    }
    return InsertsSubscriber.createInsertsSubscriber(serviceContext, properties, dataSource,
        ksqlConfig, context, acksSubscriber, workerExecutor);
  }

  private DataSource getDataSource(
      final KsqlConfig ksqlConfig,
      final MetaStore metaStore,
      final SourceName sourceName
  ) {
    final DataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream: "
          + sourceName);
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlException("Cannot insert values into windowed stream");
    }

    final ReservedInternalTopics internalTopics = new ReservedInternalTopics(ksqlConfig);
    if (internalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlException("Cannot insert values into read-only topic: "
          + dataSource.getKafkaTopicName());
    }

    return dataSource;
  }

  private static List<String> colTypesFromSchema(final LogicalSchema logicalSchema) {
    final List<Column> key = logicalSchema.key();
    final List<Column> val = logicalSchema.value();
    final List<String> colTypes = new ArrayList<>(key.size() + val.size());
    for (Column col : key) {
      colTypes.add(col.type().toString(FormatOptions.none()));
    }
    for (Column col : val) {
      colTypes.add(col.type().toString(FormatOptions.none()));
    }
    return colTypes;
  }

  private static List<String> colNamesFromSchema(final LogicalSchema logicalSchema) {
    final List<Column> key = logicalSchema.key();
    final List<Column> val = logicalSchema.value();
    final List<String> colNames = new ArrayList<>(key.size() + val.size());
    for (Column col : key) {
      colNames.add(col.name().name());
    }
    for (Column col : val) {
      colNames.add(col.name().name());
    }
    return colNames;
  }

  private static class KsqlQueryHandle implements PushQueryHandler {

    private final QueryMetadata queryMetadata;
    private final OptionalInt limit;

    KsqlQueryHandle(final QueryMetadata queryMetadata, final OptionalInt limit) {
      this.queryMetadata = queryMetadata;
      this.limit = limit;
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(queryMetadata.getLogicalSchema());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(queryMetadata.getLogicalSchema());
    }

    @Override
    public OptionalInt getLimit() {
      return limit;
    }

    @Override
    public void start() {
      queryMetadata.start();
    }

    @Override
    public void stop() {
      queryMetadata.close();
    }
  }
}
