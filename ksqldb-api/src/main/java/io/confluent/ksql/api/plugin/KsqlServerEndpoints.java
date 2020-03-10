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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.server.BaseServerEndpoints;
import io.confluent.ksql.api.server.PushQueryHandler;
import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.confluent.ksql.engine.KsqlEngine;
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
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.QueryMetadata;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.reactivestreams.Subscriber;

public class KsqlServerEndpoints extends BaseServerEndpoints {

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;
  private final ServiceContextFactory theServiceContextFactory;

  public interface ServiceContextFactory {

    ServiceContext create(
        KsqlConfig ksqlConfig,
        Optional<String> authHeader,
        KafkaClientSupplier kafkaClientSupplier,
        Supplier<SchemaRegistryClient> srClientFactory
    );
  }

  public KsqlServerEndpoints(
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension,
      final ServiceContextFactory theServiceContextFactory) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.securityExtension = Objects.requireNonNull(securityExtension);
    this.theServiceContextFactory = Objects.requireNonNull(theServiceContextFactory);
  }

  @Override
  protected PushQueryHandler createQuery(final String sql, final JsonObject properties,
      final Context context, final WorkerExecutor workerExecutor,
      final Consumer<GenericRow> rowConsumer) {
    // Must be run on worker as all this stuff is slow
    Utils.checkIsWorker();

    final ServiceContext serviceContext = createServiceContext(new DummyPrincipal());
    final ConfiguredStatement<Query> statement = createStatement(sql, properties.getMap());

    final QueryMetadata queryMetadata = ksqlEngine
        .executeQuery(serviceContext, statement, rowConsumer);
    return new KsqlQueryHandle(queryMetadata, statement.getStatement().getLimit());
  }

  private ConfiguredStatement<Query> createStatement(final String queryString,
      final Map<String, Object> properties) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      throw new KsqlStatementException(
          String.format("Expected exactly one KSQL statement; found %d instead", statements.size()),
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
    return theServiceContextFactory.create(ksqlConfig,
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
  public InsertsSubscriber createInsertsSubscriber(final String target, final JsonObject properties,
      final Subscriber<JsonObject> acksSubscriber) {
    return null;
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

    private static List<String> colTypesFromSchema(final LogicalSchema logicalSchema) {
      final List<Column> cols = logicalSchema.value();
      final List<String> colTypes = new ArrayList<>(cols.size());
      for (Column col : cols) {
        colTypes.add(col.type().toString(FormatOptions.none()));
      }
      return colTypes;
    }

    private static List<String> colNamesFromSchema(final LogicalSchema logicalSchema) {
      final List<Column> cols = logicalSchema.value();
      final List<String> colNames = new ArrayList<>(cols.size());
      for (Column col : cols) {
        colNames.add(col.name().name());
      }
      return colNames;
    }
  }
}
