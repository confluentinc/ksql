package io.confluent.ksql.test;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.test.tools.TestFunctionRegistry;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

public class StatementRunner {

  private static final ImmutableMap<String, Object> BASE_CONFIG = ImmutableMap
      .<String, Object>builder()
      .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
      .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
      .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
      .put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0L)
      .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
      .put(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .build();
  final private ServiceContext serviceContext;
  final private KsqlEngine engine;
  final private KsqlConfig config;
  final private Injector formatInjector;
  final private FakeKafkaTopicClient topicClient;

  public StatementRunner() {
    final MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
    this.topicClient = new FakeKafkaTopicClient();
    this.serviceContext = TestServiceContext.create(topicClient, () -> srClient);
    this.config = new KsqlConfig(BASE_CONFIG);
    this.formatInjector = new DefaultFormatInjector();

    final MetaStoreImpl metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    final MetricCollectors metricCollectors = new MetricCollectors();
    this.engine = new KsqlEngine(
        serviceContext,
        NoopProcessingLogContext.INSTANCE,
        metaStore,
        ServiceInfo.create(config),
        new SequentialQueryIdGenerator(),
        this.config,
        Collections.singletonList(
            new QueryEventListener() {
              @Override
              public void onDeregister(QueryMetadata query) {
              }
            }
        ),
        metricCollectors
    );
  }

  public MetaStore getMetaStore() {
    return engine.getMetaStore();
  }

  public void runStatements(final List<ParsedStatement> statements) {
    statements.forEach(statement -> runStatement(statement));
  }

  private void runStatement(final ParsedStatement parsedStatement) {
    final PreparedStatement<?> engineStatement = engine.prepare(parsedStatement);
    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(engineStatement, SessionConfig.of(config, Collections.EMPTY_MAP));
    createTopics(engineStatement);
    final ConfiguredStatement<?> injected = formatInjector.inject(configured);
    engine.execute(
        serviceContext,
        injected);
  }

  private void createTopics(final PreparedStatement<?> engineStatement) {
    if (engineStatement.getStatement() instanceof CreateSource) {
      final CreateSource statement = (CreateSource) engineStatement.getStatement();
      topicClient.preconditionTopicExists(
          statement.getProperties().getKafkaTopic(),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    } else if (engineStatement.getStatement() instanceof CreateAsSelect) {
      final CreateAsSelect statement = (CreateAsSelect) engineStatement.getStatement();
      topicClient.preconditionTopicExists(
          statement.getProperties().getKafkaTopic()
              .orElse(statement.getName().toString(FormatOptions.noEscape()).toUpperCase()),
          statement.getProperties().getPartitions().orElse(1),
          statement.getProperties().getReplicas().orElse((short) 1),
          ImmutableMap.of()
      );
    }
  }

  public void close() {
    serviceContext.close();
    engine.close();
  }
}
