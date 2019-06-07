/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.physical;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PhysicalPlanBuilderTest {

  private final String simpleSelectFilter = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
  private PhysicalPlanBuilder physicalPlanBuilder;
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private LogicalPlanBuilder planBuilder;
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
      = new MockSchemaRegistryClientFactory()::get;
  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
          "application.id", "KSQL",
          "commit.interval.ms", 0,
          "cache.max.bytes.buffering", 0,
          "auto.offset.reset", "earliest"));

  // Test implementation of KafkaStreamsBuilder that tracks calls and returned values
  class TestKafkaStreamsBuilder implements KafkaStreamsBuilder {

    class Call {

      public StreamsBuilder builder;
      public StreamsConfig config;
      KafkaStreams kafkaStreams;

      private Call(final StreamsBuilder builder, final StreamsConfig config, final KafkaStreams kafkaStreams) {
        this.builder = builder;
        this.config = config;
        this.kafkaStreams = kafkaStreams;
      }
    }

    private List<Call> calls = new LinkedList<>();

    @Override
    public KafkaStreams buildKafkaStreams(final StreamsBuilder builder, final StreamsConfig conf) {
      final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), conf);
      calls.add(new Call(builder, conf, kafkaStreams));
      return kafkaStreams;
    }

    List<Call> getCalls() {
      return calls;
    }
  }

  private TestKafkaStreamsBuilder testKafkaStreamsBuilder;

  @Before
  public void before() {
    testKafkaStreamsBuilder = new TestKafkaStreamsBuilder();
    physicalPlanBuilder = buildPhysicalPlanBuilder(Collections.emptyMap());
    planBuilder = new LogicalPlanBuilder(metaStore);
  }

  private PhysicalPlanBuilder buildPhysicalPlanBuilder(final Map<String, Object> overrideProperties) {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    return new PhysicalPlanBuilder(
        streamsBuilder,
        ksqlConfig,
        new FakeKafkaTopicClient(),
        functionRegistry,
        overrideProperties,
        false,
        metaStore,
        schemaRegistryClientFactory,
        new QueryIdGenerator(),
        testKafkaStreamsBuilder
    );

  }

  private QueryMetadata buildPhysicalPlan(final String query) throws Exception {
    final PlanNode logical = planBuilder.buildLogicalPlan(query);
    return physicalPlanBuilder.buildPhysicalPlan(new Pair<>(query, logical));
  }

  @Test
  public void shouldHaveKStreamDataSource() throws Exception {
    final QueryMetadata metadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(metadata.getDataSourceType(), equalTo(DataSource.DataSourceType.KSTREAM));
  }

  @Test
  public void shouldHaveOutputNode() throws Exception {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(queryMetadata.getOutputNode(), instanceOf(KsqlBareOutputNode.class));
  }

  @Test
  public void shouldCreateExecutionPlan() throws Exception {
    final String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 "
        + "WHERE col0 > 100 GROUP BY col0;";
    final QueryMetadata metadata = buildPhysicalPlan(queryString);
    final String planText = metadata.getExecutionPlan();
    final String[] lines = planText.split("\n");
    Assert.assertEquals(" > [ SINK ] Schema: [COL0 : BIGINT, KSQL_COL_1 : DOUBLE"
        + ", KSQL_COL_2 : BIGINT].", lines[0]);
    Assert.assertEquals(
        "\t\t > [ AGGREGATE ] Schema: [KSQL_INTERNAL_COL_0 : BIGINT, KSQL_INTERNAL_COL_1 : DOUBLE, KSQL_AGG_VARIABLE_0 : DOUBLE, KSQL_AGG_VARIABLE_1 : BIGINT].",
        lines[1]);
    Assert.assertEquals(
        "\t\t\t\t > [ PROJECT ] Schema: [KSQL_INTERNAL_COL_0 : BIGINT, KSQL_INTERNAL_COL_1 : DOUBLE, KSQL_INTERNAL_COL_2 : DOUBLE, KSQL_INTERNAL_COL_3 : DOUBLE].",
        lines[2]);
    Assert.assertEquals(
        "\t\t\t\t\t\t > [ FILTER ] Schema: [TEST1.ROWTIME : BIGINT, TEST1.ROWKEY : BIGINT, TEST1.COL0 : BIGINT, TEST1.COL1 : VARCHAR, TEST1.COL2 : VARCHAR, TEST1.COL3 : DOUBLE, TEST1.COL4 : ARRAY<DOUBLE>, TEST1.COL5 : MAP<VARCHAR,DOUBLE>].",
        lines[3]);
    Assert.assertEquals(
        "\t\t\t\t\t\t\t\t > [ SOURCE ] Schema: [TEST1.ROWTIME : BIGINT, TEST1.ROWKEY : BIGINT, TEST1.COL0 : BIGINT, TEST1.COL1 : VARCHAR, TEST1.COL2 : VARCHAR, TEST1.COL3 : DOUBLE, TEST1.COL4 : ARRAY<DOUBLE>, TEST1.COL5 : MAP<VARCHAR,DOUBLE>].",
        lines[4]);
  }

  @Test
  public void shouldCreateExecutionPlanForInsert() throws Exception {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        createStream + "\n " + csasQuery + "\n " + insertIntoQuery,
        ksqlConfig,
        Collections.emptyMap());
    Assert.assertTrue(queryMetadataList.size() == 2);
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    Assert.assertTrue(lines.length == 3);
    Assert.assertEquals(lines[0],
        " > [ SINK ] Schema: [COL0 : BIGINT, COL1 : VARCHAR, COL2 : DOUBLE].");
    Assert.assertEquals(lines[1],
        "\t\t > [ PROJECT ] Schema: [COL0 : BIGINT, COL1 : VARCHAR, COL2 : DOUBLE].");
    Assert.assertEquals(lines[2],
        "\t\t\t\t > [ SOURCE ] Schema: [TEST1.ROWTIME : BIGINT, TEST1.ROWKEY : VARCHAR, TEST1.COL0 : BIGINT, TEST1.COL1 : VARCHAR, TEST1.COL2 : DOUBLE].");
    assertThat(queryMetadataList.get(1).getOutputNode(),
        instanceOf(KsqlStructuredDataOutputNode.class));
    final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode = (KsqlStructuredDataOutputNode)
        queryMetadataList.get(1).getOutputNode();
    assertThat(ksqlStructuredDataOutputNode.getKsqlTopic().getKsqlTopicSerDe().getSerDe(),
        equalTo(DataSource.DataSourceSerDe.DELIMITED));
  }

  @Test
  public void shouldFailIfInsertSinkDoesNotExist() throws Exception {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    final KsqlEngine ksqlEngine = new KsqlEngine(
        new FakeKafkaTopicClient(),
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));
    try {
      final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
          createStream + "\n " + insertIntoQuery,
          ksqlConfig,
          Collections.emptyMap());
    } catch (final KsqlException ksqlException) {
      assertThat(ksqlException.getMessage(), equalTo("Exception while processing statements "
          + ":Sink, S1, does not exist for the INSERT INTO statement."));
      return;
    }
    Assert.fail();

  }

  @Test
  public void shouldFailInsertIfTheResultSchemaDoesNotMatch() throws Exception {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2, col3  FROM test1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    try {
      final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
          createStream + "\n " + csasQuery + "\n " + insertIntoQuery,
          ksqlConfig,
          Collections.emptyMap());
    } catch (final KsqlException ksqlException) {
      assertThat(ksqlException.getMessage(),
          equalTo("Incompatible schema between results and sink. Result schema is [COL0 :"
              + " BIGINT, COL1 : VARCHAR, COL2 : DOUBLE, COL3 : DOUBLE], but the sink schema is [COL0 : BIGINT, COL1 : VARCHAR, COL2 : DOUBLE]."));
      return;
    }
    Assert.fail();
  }

  @Test
  public void shouldCreatePlanForInsertIntoTableFromTable() throws Exception {
    final String createTable = "CREATE TABLE T1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON', KEY = 'COL1' );";
    final String csasQuery = "CREATE TABLE T2 AS SELECT * FROM T1;";
    final String insertIntoQuery = "INSERT INTO T2 SELECT *  FROM T1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        createTable + "\n " + csasQuery + "\n " + insertIntoQuery,
        ksqlConfig,
        Collections.emptyMap());
    Assert.assertTrue(queryMetadataList.size() == 2);
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(2));
    assertThat(lines[0],
        equalTo(" > [ PROJECT ] Schema: [ROWTIME : BIGINT, ROWKEY : VARCHAR, COL0 : "
            + "BIGINT, COL1 : VARCHAR, COL2 : DOUBLE, COL3 : DOUBLE]."));
    assertThat(lines[1],
        equalTo("\t\t > [ SOURCE ] Schema: [T1.ROWTIME : BIGINT, T1.ROWKEY : VARCHAR, "
            + "T1.COL0 : BIGINT, T1.COL1 : VARCHAR, T1.COL2 : DOUBLE, T1.COL3 : "
            + "DOUBLE]."));
  }

  @Test
  public void shouldFailInsertIfTheResultTypesDontMatch() throws Exception {
    final String createTable = "CREATE TABLE T1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 't1', VALUE_FORMAT = 'JSON', KEY = 'COL1' );";
    final String createStream = "CREATE STREAM S1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 's1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM S2 AS SELECT * FROM S1;";
    final String insertIntoQuery = "INSERT INTO S2 SELECT col0, col1, col2, col3 FROM T1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    // No need for setting the correct clean up policy in test.
    kafkaTopicClient.createTopic("t1", 1, (short) 1, Collections.emptyMap());
    kafkaTopicClient.createTopic("s1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    try {
      final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
          createTable + "\n " + createStream + "\n " + csasQuery + "\n " + insertIntoQuery,
          ksqlConfig,
          Collections.emptyMap());
    } catch (final KsqlException ksqlException) {
      assertThat(ksqlException.getMessage(), equalTo("Incompatible data sink and query result. "
          + "Data sink (S2) type is KTABLE but select query result is KSTREAM."));
      return;
    }
    Assert.fail();
  }

  @Test
  public void shouldCheckSinkAndResultKeysDoNotMatch() throws Exception {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
        createStream + "\n " + csasQuery + "\n " + insertIntoQuery,
        ksqlConfig,
        Collections.emptyMap());
    Assert.assertTrue(queryMetadataList.size() == 2);
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(4));
    assertThat(lines[0],
        equalTo(" > [ REKEY ] Schema: [COL0 : BIGINT, COL1 : VARCHAR, COL2 : DOUBLE]."));
    assertThat(lines[1], equalTo("\t\t > [ SINK ] Schema: [COL0 : BIGINT, COL1 : VARCHAR, COL2 "
        + ": DOUBLE]."));
    assertThat(lines[2], equalTo("\t\t\t\t > [ PROJECT ] Schema: [COL0 : BIGINT, COL1 : VARCHAR"
        + ", COL2 : DOUBLE]."));
  }

  @Test
  public void shouldFailIfSinkAndResultKeysDoNotMatch() throws Exception {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    try {
      final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
          createStream + "\n " + csasQuery + "\n " + insertIntoQuery,
          ksqlConfig,
          Collections.emptyMap());
    } catch (final Exception ksqlException) {
      assertThat(ksqlException.getMessage(), equalTo("Incompatible key fields for sink and "
          + "results. Sink key field is COL0 (type: "
          + "Schema{INT64}) while result key field is null (type: null)"));
      return;
    }
    Assert.fail();
  }


  @Test
  public void shouldReturnCreatedKafkaStream() throws Exception {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    assertThat(1, equalTo(calls.size()));
    Assert.assertSame(calls.get(0).kafkaStreams, queryMetadata.getKafkaStreams());
  }

  @Test
  public void shouldAddMetricsInterceptors() throws Exception {
    buildPhysicalPlan(simpleSelectFilter);

    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    final StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(1));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(0))));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> producerInterceptors = (List<String>) val;
    assertThat(producerInterceptors.size(), equalTo(1));
    assertThat(ProducerCollector.class, equalTo(Class.forName(producerInterceptors.get(0))));
  }

  public static class DummyConsumerInterceptor implements ConsumerInterceptor {

    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  public static class DummyProducerInterceptor implements ProducerInterceptor {

    public void onAcknowledgement(final RecordMetadata rm, final Exception e) {
    }

    public ProducerRecord onSend(final ProducerRecord producerRecords) {
      return producerRecords;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingList() throws Exception {
    // Initialize override properties with lists for producer/consumer interceptors
    final Map<String, Object> overrideProperties = new HashMap<>();
    List<String> consumerInterceptors = new LinkedList<>();
    consumerInterceptors.add(DummyConsumerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        consumerInterceptors);
    List<String> producerInterceptors = new LinkedList<>();
    producerInterceptors.add(DummyProducerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        producerInterceptors);
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    final StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(2));
    assertThat(DummyConsumerInterceptor.class.getName(), equalTo(consumerInterceptors.get(0)));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(1))));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    producerInterceptors = (List<String>) val;
    assertThat(producerInterceptors.size(), equalTo(2));
    assertThat(DummyProducerInterceptor.class.getName(), equalTo(producerInterceptors.get(0)));
    assertThat(ProducerCollector.class, equalTo(Class.forName(producerInterceptors.get(1))));
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingString() throws Exception {
    // Initialize override properties with class name strings for producer/consumer interceptors
    final Map<String, Object> overrideProperties = new HashMap<>();
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyConsumerInterceptor.class.getName());
    overrideProperties.put(StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        DummyProducerInterceptor.class.getName());
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    assertThat(calls.size(), equalTo(1));
    final StreamsConfig config = calls.get(0).config;

    Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(2));
    assertThat(DummyConsumerInterceptor.class.getName(), equalTo(consumerInterceptors.get(0)));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(1))));

    val = config.originals().get(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> producerInterceptors = (List<String>) val;
    assertThat(producerInterceptors.size(), equalTo(2));
    assertThat(DummyProducerInterceptor.class.getName(), equalTo(producerInterceptors.get(0)));
    assertThat(ProducerCollector.class, equalTo(Class.forName(producerInterceptors.get(1))));
  }

  public static class DummyConsumerInterceptor2 implements ConsumerInterceptor {

    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  @Test
  public void shouldAddMetricsInterceptorsToExistingStringList() throws Exception {
    // Initialize override properties with class name strings for producer/consumer interceptors
    final Map<String, Object> overrideProperties = new HashMap<>();
    final String consumerInterceptorStr = DummyConsumerInterceptor.class.getName()
        + " , " + DummyConsumerInterceptor2.class.getName();
    overrideProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        consumerInterceptorStr);
    physicalPlanBuilder = buildPhysicalPlanBuilder(overrideProperties);

    buildPhysicalPlan(simpleSelectFilter);

    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    final StreamsConfig config = calls.get(0).config;

    final Object val = config.originals().get(
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(3));
    assertThat(DummyConsumerInterceptor.class.getName(), equalTo(consumerInterceptors.get(0)));
    assertThat(DummyConsumerInterceptor2.class.getName(), equalTo(consumerInterceptors.get(1)));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(2))));
  }

  @Test
  public void shouldCreateExpectedServiceId() {
    final String serviceId = physicalPlanBuilder.getServiceId();
    assertThat(serviceId, equalTo(KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
        + KsqlConfig.KSQL_SERVICE_ID_DEFAULT));
  }

  @Test
  public void shouldHaveOptionalFieldsInResultSchema() {
    final String createStream = "CREATE STREAM TEST1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON' );";
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaTopicClient.createTopic("test1", 1, (short) 1, Collections.emptyMap());
    final KsqlEngine ksqlEngine = new KsqlEngine(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        new MetaStoreImpl(new InternalFunctionRegistry()));

    final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(createStream + "\n " +
        csasQuery + "\n " +
        insertIntoQuery,
        ksqlConfig,
        Collections.emptyMap());
    final Schema resultSchema = queryMetadataList.get(0).getOutputNode().getSchema();
    resultSchema.fields().stream().forEach(
        field -> Assert.assertTrue(field.schema().isOptional())
    );
  }
}
