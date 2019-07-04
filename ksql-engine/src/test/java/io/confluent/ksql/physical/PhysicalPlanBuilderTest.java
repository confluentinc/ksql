/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.physical;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.hasSerdeOptions;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static io.confluent.ksql.util.KsqlExceptionMatcher.rawMessage;
import static io.confluent.ksql.util.KsqlExceptionMatcher.statementText;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanTestUtil;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PhysicalPlanBuilderTest {

  private static final String FILTER_NODE = "KSTREAM-FILTER-0000000003";
  private static final String FILTER_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000004";
  private static final String FOREACH_NODE = "KSTREAM-FOREACH-0000000005";

  private static final String CREATE_STREAM_TEST1 = "CREATE STREAM TEST1 "
      + "(COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) "
      + "WITH (KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST2 = "CREATE STREAM TEST2 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test2', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_STREAM_TEST3 = "CREATE STREAM TEST3 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test3', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_TABLE_TEST4 = "CREATE TABLE TEST4 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test4', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_TABLE_TEST5 = "CREATE TABLE TEST5 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test5', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_STREAM_TEST6 = "CREATE STREAM TEST6 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test6', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST7 = "CREATE STREAM TEST7 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test7', VALUE_FORMAT = 'JSON');";

  private static final String simpleSelectFilter = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
  private PhysicalPlanBuilder physicalPlanBuilder;
  private final MutableMetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private static final KsqlConfig INITIAL_CONFIG = KsqlConfigTestUtil.create("what-eva");
  private final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
  private KsqlEngine ksqlEngine;
  private ProcessingLogContext processingLogContext;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private ServiceContext serviceContext;
  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;

  private KsqlConfig ksqlConfig;
  private MetaStoreImpl engineMetastore;

  // Test implementation of KafkaStreamsBuilder that tracks calls and returned values
  private static class TestKafkaStreamsBuilder implements KafkaStreamsBuilder {

    private final ServiceContext serviceContext;

    private TestKafkaStreamsBuilder(final ServiceContext serviceContext) {
      this.serviceContext = serviceContext;
    }

    private static class Call {
      private final Properties props;

      private Call(final Properties props) {
        this.props = props;
      }
    }

    private final List<Call> calls = new LinkedList<>();

    @Override
    public KafkaStreams buildKafkaStreams(final StreamsBuilder builder, final Map<String, Object> conf) {
      final Properties props = new Properties();
      props.putAll(conf);
      final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props, serviceContext.getKafkaClientSupplier());
      calls.add(new Call(props));
      return kafkaStreams;
    }

    List<Call> getCalls() {
      return calls;
    }
  }

  private TestKafkaStreamsBuilder testKafkaStreamsBuilder;

  @Before
  public void before() {
    ksqlConfig = INITIAL_CONFIG;
    serviceContext = TestServiceContext.create(kafkaTopicClient);
    processingLogContext = ProcessingLogContext.create();
    testKafkaStreamsBuilder = new TestKafkaStreamsBuilder(serviceContext);
    physicalPlanBuilder = buildPhysicalPlanBuilder(Collections.emptyMap());
    engineMetastore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        engineMetastore
    );
  }

  @After
  public void after() {
    ksqlEngine.close();
    serviceContext.close();
  }

  private PhysicalPlanBuilder buildPhysicalPlanBuilder(
      final Map<String, Object> overrideProperties) {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    return new PhysicalPlanBuilder(
        streamsBuilder,
        ksqlConfig.cloneWithPropertyOverwrite(overrideProperties),
        serviceContext,
        processingLogContext,
        functionRegistry,
        overrideProperties,
        metaStore,
        new QueryIdGenerator(),
        testKafkaStreamsBuilder,
        queryCloseCallback
    );
  }

  private QueryMetadata buildPhysicalPlan(final String query) {
    final OutputNode logical = AnalysisTestUtil.buildLogicalPlan(query, metaStore);;
    return physicalPlanBuilder.buildPhysicalPlan(new LogicalPlanNode(query, Optional.of(logical)));
  }

  @Test
  public void shouldHaveKStreamDataSource() {
    final QueryMetadata metadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(metadata.getDataSourceType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldMakeBareQuery() {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(queryMetadata, instanceOf(TransientQueryMetadata.class));
  }

  @Test
  public void shouldBuildTransientQueryWithCorrectSchema() {
    // When:
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);

    // Then:
    assertThat(queryMetadata.getLogicalSchema(), is(LogicalSchema.of(
        SchemaBuilder.struct()
            .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
            .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build()
    )));
  }

  @Test
  public void shouldBuildPersistentQueryWithCorrectSchema() {
    // When:
    final QueryMetadata queryMetadata = buildPhysicalPlan(
        "CREATE STREAM FOO AS " + simpleSelectFilter);

    // Then:
    assertThat(queryMetadata.getLogicalSchema(), is(LogicalSchema.of(
        SchemaBuilder.struct()
            .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
            .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build()
    )));
  }

  @Test
  public void shouldMakePersistentQuery() {
    // Given:
    givenKafkaTopicsExist("test1");

    // When:
    final QueryMetadata queryMetadata =
        buildPhysicalPlan("CREATE STREAM FOO AS " + simpleSelectFilter);

    // Then:
    assertThat(queryMetadata, instanceOf(PersistentQueryMetadata.class));
  }

  @Test
  public void shouldBuildMapValuesNodeForTransientQueries() {
    // Given:
    final QueryMetadata query = buildPhysicalPlan(simpleSelectFilter);

    // When:
    final TopologyDescription.Processor node = getNodeByName(query, FILTER_MAPVALUES_NODE);

    // Then:
    verifyProcessorNode(node, ImmutableList.of(FILTER_NODE), ImmutableList.of(FOREACH_NODE));
  }

  @Test
  public void shouldBuildForEachNodeForTransientQueries() {
    // Given:
    final QueryMetadata query = buildPhysicalPlan(simpleSelectFilter);

    // When:
    final TopologyDescription.Processor node = getNodeByName(query, FOREACH_NODE);

    // Then:
    verifyProcessorNode(node, ImmutableList.of(FILTER_MAPVALUES_NODE), ImmutableList.of());
  }

  @Test
  public void shouldCreateExecutionPlan() {
    final String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 "
        + "WHERE col0 > 100 GROUP BY col0;";
    final QueryMetadata metadata = buildPhysicalPlan(queryString);
    final String planText = metadata.getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines[0], startsWith(
        " > [ SINK ] | Schema: [COL0 BIGINT, KSQL_COL_1 DOUBLE, KSQL_COL_2 BIGINT] |"));
    assertThat(lines[1], startsWith(
        "\t\t > [ AGGREGATE ] | Schema: [KSQL_INTERNAL_COL_0 BIGINT, "
            + "KSQL_INTERNAL_COL_1 DOUBLE, KSQL_AGG_VARIABLE_0 DOUBLE, "
            + "KSQL_AGG_VARIABLE_1 BIGINT] |"));
    assertThat(lines[2], startsWith(
        "\t\t\t\t > [ PROJECT ] | Schema: [KSQL_INTERNAL_COL_0 BIGINT, "
            + "KSQL_INTERNAL_COL_1 DOUBLE] |"));
    assertThat(lines[3], startsWith(
        "\t\t\t\t\t\t > [ FILTER ] | Schema: [TEST1.ROWTIME BIGINT, TEST1.ROWKEY VARCHAR, "
            + "TEST1.COL0 BIGINT, TEST1.COL1 VARCHAR, TEST1.COL2 VARCHAR, "
            + "TEST1.COL3 DOUBLE, TEST1.COL4 ARRAY<DOUBLE>, "
            + "TEST1.COL5 MAP<VARCHAR, DOUBLE>] |"));
    assertThat(lines[4], startsWith(
        "\t\t\t\t\t\t\t\t > [ SOURCE ] | Schema: [TEST1.ROWTIME BIGINT, TEST1.ROWKEY VARCHAR, "
            + "TEST1.COL0 BIGINT, TEST1.COL1 VARCHAR, TEST1.COL2 VARCHAR, "
            + "TEST1.COL3 DOUBLE, TEST1.COL4 ARRAY<DOUBLE>, "
            + "TEST1.COL5 MAP<VARCHAR, DOUBLE>] |"));
  }

  @Test
  public void shouldCreateExecutionPlanForInsert() {
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    Assert.assertTrue(queryMetadataList.size() == 2);
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    Assert.assertTrue(lines.length == 3);
    Assert.assertEquals(lines[0],
        " > [ SINK ] | Schema: [COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE] | Logger: InsertQuery_1.S1");
    Assert.assertEquals(lines[1],
        "\t\t > [ PROJECT ] | Schema: [COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE] | Logger: InsertQuery_1.Project");
    Assert.assertEquals(lines[2],
        "\t\t\t\t > [ SOURCE ] | Schema: [TEST1.ROWTIME BIGINT, TEST1.ROWKEY VARCHAR, TEST1.COL0 BIGINT, TEST1.COL1 VARCHAR, TEST1.COL2 DOUBLE] | Logger: InsertQuery_1.KsqlTopic");
    assertThat(queryMetadataList.get(1), instanceOf(PersistentQueryMetadata.class));
    final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata)
        queryMetadataList.get(1);
    assertThat(persistentQuery.getResultTopic().getValueSerdeFactory().getFormat(),
        equalTo(Format.DELIMITED));
  }

  @Test
  public void shouldFailIfInsertSinkDoesNotExist() {
    // Given:
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    // Then:
    expectedException.expect(ParseFailedException.class);
    expectedException.expect(statementText(is("INSERT INTO s1 SELECT col0, col1, col2 FROM test1;")));
    expectedException.expect(rawMessage(containsString("S1 does not exist.")));

    // When:
    execute(CREATE_STREAM_TEST1 + insertIntoQuery);
  }

  @Test
  public void shouldFailInsertIfTheResultSchemaDoesNotMatch() {
    // Given:
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1 FROM test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Incompatible schema between results and sink. Result schema is "
            + "[`COL0` BIGINT, `COL1` VARCHAR, `COL2` DOUBLE], "
            + "but the sink schema is "
            + "[`COL0` BIGINT, `COL1` VARCHAR].")));

    // When:
    execute(CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
  }

  @Test
  public void shouldThrowOnInsertIntoTableFromTable() {
    // Given:
    final String createTable = "CREATE TABLE T1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON', KEY = 'COL1' );";
    final String csasQuery = "CREATE TABLE T2 AS SELECT * FROM T1;";
    final String insertIntoQuery = "INSERT INTO T2 SELECT *  FROM T1;";
    givenKafkaTopicsExist("test1");

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "INSERT INTO can only be used to insert into a stream. T2 is a table.")));

    // When:
    execute(createTable + csasQuery + insertIntoQuery);
  }

  @Test
  public void shouldCreatePlanForInsertIntoStreamFromStream() {
    // Given:
    final String cs = "CREATE STREAM test1 (col0 INT) "
        + "WITH (KAFKA_TOPIC='test1', VALUE_FORMAT='JSON');";
    final String csas = "CREATE STREAM s0 AS SELECT * FROM test1;";
    final String insertInto = "INSERT INTO s0 SELECT * FROM test1;";
    givenKafkaTopicsExist("test1");

    // When:
    final List<QueryMetadata> queries = execute(cs + csas + insertInto);

    // Then:
    assertThat(queries, hasSize(2));
    final String planText = queries.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(3));
    assertThat(lines[0], containsString(
        "> [ SINK ] | Schema: [ROWTIME BIGINT, ROWKEY VARCHAR, COL0 INT]"));

    assertThat(lines[1], containsString(
        "> [ PROJECT ] | Schema: [ROWTIME BIGINT, ROWKEY VARCHAR, COL0 INT]"));

    assertThat(lines[2], containsString(
        "> [ SOURCE ] | Schema: [TEST1.ROWTIME BIGINT, TEST1.ROWKEY VARCHAR, TEST1.COL0 INT]"));
  }

  @Test
  public void shouldFailInsertIfTheResultTypesDoNotMatch() {
    // Given:
    final String createTable = "CREATE TABLE T1 (COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE, COL3 "
        + "DOUBLE) "
        + "WITH ( "
        + "KAFKA_TOPIC = 't1', VALUE_FORMAT = 'JSON', KEY = 'COL1' );";
    final String csasQuery = "CREATE STREAM S2 AS SELECT * FROM TEST1;";
    final String insertIntoQuery = "INSERT INTO S2 SELECT col0, col1, col2, col3 FROM T1;";
    // No need for setting the correct clean up policy in test.
    givenKafkaTopicsExist("t1");
    givenKafkaTopicsExist("test1");

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Incompatible data sink and query result. "
        + "Data sink (S2) type is KTABLE but select query result is KSTREAM.")));

    // When:
    execute(createTable + CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
  }

  @Test
  public void shouldCheckSinkAndResultKeysDoNotMatch() {
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    givenKafkaTopicsExist("test1");

    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    assertThat(queryMetadataList, hasSize(2));
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(4));
    assertThat(lines[0],
        equalTo(" > [ REKEY ] | Schema: [COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE] "
            + "| Logger: InsertQuery_1.S1"));
    assertThat(lines[1], equalTo("\t\t > [ SINK ] | Schema: [COL0 BIGINT, COL1 VARCHAR, COL2 "
        + "DOUBLE] | Logger: InsertQuery_1.S1"));
    assertThat(lines[2], equalTo("\t\t\t\t > [ PROJECT ] | Schema: [COL0 BIGINT, COL1 VARCHAR"
        + ", COL2 DOUBLE] | Logger: InsertQuery_1.Project"));
  }

  @Test
  public void shouldFailIfSinkAndResultKeysDoNotMatch() {
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Incompatible key fields for sink and results. "
        + "Sink key field is COL0 (type: Schema{INT64}) "
        + "while result key field is null (type: null)")));

    // When:
    execute(CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
  }

  @Test
  public void shouldAddMetricsInterceptors() throws Exception {
    buildPhysicalPlan(simpleSelectFilter);

    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    Assert.assertEquals(1, calls.size());
    final Properties props = calls.get(0).props;

    Object val = props.get(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(1));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(0))));

    val = props.get(StreamsConfig.producerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> producerInterceptors = (List<String>) val;
    assertThat(producerInterceptors.size(), equalTo(1));
    assertThat(ProducerCollector.class, equalTo(Class.forName(producerInterceptors.get(0))));
  }

  private void shouldUseProvidedOptimizationConfig(Object value) {
    // Given:
    final Map<String, Object> properties =
        Collections.singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION, value);
    physicalPlanBuilder = buildPhysicalPlanBuilder(properties);

    // When:
    buildPhysicalPlan(simpleSelectFilter);

    // Then:
    final List<TestKafkaStreamsBuilder.Call> calls = testKafkaStreamsBuilder.getCalls();
    assertThat(calls.size(), equalTo(1));
    final Properties props = calls.get(0).props;
    assertThat(
        props.get(StreamsConfig.TOPOLOGY_OPTIMIZATION),
        equalTo(value));
  }

  @Test
  public void shouldUseOptimizationConfigProvidedWhenOn() {
    shouldUseProvidedOptimizationConfig(StreamsConfig.OPTIMIZE);
  }

  @Test
  public void shouldUseOptimizationConfigProvidedWhenOff() {
    shouldUseProvidedOptimizationConfig(StreamsConfig.NO_OPTIMIZATION);
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
    final Properties props = calls.get(0).props;

    Object val = props.get(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(2));
    assertThat(DummyConsumerInterceptor.class.getName(), equalTo(consumerInterceptors.get(0)));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(1))));

    val = props.get(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
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
    final Properties props = calls.get(0).props;

    Object val = props.get(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG));
    Assert.assertThat(val, instanceOf(List.class));
    final List<String> consumerInterceptors = (List<String>) val;
    assertThat(consumerInterceptors.size(), equalTo(2));
    assertThat(DummyConsumerInterceptor.class.getName(), equalTo(consumerInterceptors.get(0)));
    assertThat(ConsumerCollector.class, equalTo(Class.forName(consumerInterceptors.get(1))));

    val = props.get(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG));
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
  public void shouldConfigureProducerErrorHandlerLogger() {
    // Given:
    processingLogContext = mock(ProcessingLogContext.class);
    final ProcessingLoggerFactory loggerFactory = mock(ProcessingLoggerFactory.class);
    final ProcessingLogger logger = mock(ProcessingLogger.class);
    when(processingLogContext.getLoggerFactory()).thenReturn(loggerFactory);
    final OutputNode spyNode = spy(
        AnalysisTestUtil.buildLogicalPlan(simpleSelectFilter, metaStore));
    doReturn(new QueryId("foo")).when(spyNode).getQueryId(any());
    when(loggerFactory.getLogger("foo")).thenReturn(logger);
    when(loggerFactory.getLogger(ArgumentMatchers.startsWith("foo.")))
        .thenReturn(mock(ProcessingLogger.class));
    physicalPlanBuilder = buildPhysicalPlanBuilder(Collections.emptyMap());

    // When:
    physicalPlanBuilder.buildPhysicalPlan(
        new LogicalPlanNode(simpleSelectFilter, Optional.of(spyNode)));

    // Then:
    final TestKafkaStreamsBuilder.Call call = testKafkaStreamsBuilder.calls.get(0);
    assertThat(
        call.props.get(ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER),
        is(logger));
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
    final Properties props = calls.get(0).props;

    final Object val = props.get(
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
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");
    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    final LogicalSchema resultSchema = queryMetadataList.get(0).getLogicalSchema();
    resultSchema.valueFields().forEach(
        field -> Assert.assertTrue(field.schema().isOptional())
    );
  }

  @Test
  public void shouldSetIsKSQLSinkInMetastoreCorrectly() {
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1;";
    final String ctasQuery = "CREATE TABLE t1 AS SELECT col0, COUNT(*) FROM test1 GROUP BY col0;";
    givenKafkaTopicsExist("test1");
    execute(CREATE_STREAM_TEST1 + csasQuery + ctasQuery);
    assertThat(ksqlEngine.getMetaStore().getSource("TEST1").getKsqlTopic().isKsqlSink(), equalTo(false));
    assertThat(ksqlEngine.getMetaStore().getSource("S1").getKsqlTopic().isKsqlSink(), equalTo(true));
    assertThat(ksqlEngine.getMetaStore().getSource("T1").getKsqlTopic().isKsqlSink(), equalTo(true));
  }

  @Test
  public void shouldRepartitionLeftStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.col1 = test3.id;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST2.ROWTIME BIGINT"));
  }

  @Test
  public void shouldRepartitionRightStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id = test3.col0;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST3.ROWTIME BIGINT"));
  }

  @Test
  public void shouldThrowIfLeftTableNotJoiningOnTableKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST4) key column (TEST4.ID) is not the column "
            + "used in the join criteria (TEST4.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.col0 = test5.id;");
  }

  @Test
  public void shouldThrowIfRightTableNotJoiningOnTableKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST5) key column (TEST5.ID) is not the column "
            + "used in the join criteria (TEST5.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.col0;");
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnKeys() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id = test3.id;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.rowkey = test3.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnRowKeyEvenIfStreamsHaveNoKeyField() {
    // Given:
    givenKafkaTopicsExist("test6", "test7");
    execute(CREATE_STREAM_TEST6 + CREATE_STREAM_TEST7);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test6 JOIN test7 WITHIN 1 SECOND "
        + "ON test6.rowkey = test7.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldHandleLeftTableJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.rowkey = test5.id;");

    // Then: did not throw.
  }

  @Test
  public void shouldHandleRightTableJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.rowkey;");

    // Then: did not throw.
  }

  @Test
  public void shouldRepartitionLeftStreamIfNotCorrectKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.col1 = test3.id;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST2.ROWTIME BIGINT"));
  }

  @Test
  public void shouldRepartitionRightStreamIfNotCorrectKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id = test3.col0;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST3.ROWTIME BIGINT"));
  }

  @Test
  public void shouldThrowIfLeftTableNotJoiningOnTableKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST4) key column (ID) is not the column "
            + "used in the join criteria (TEST4.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.col0 = test5.id;");
  }

  @Test
  public void shouldThrowIfRightTableNotJoiningOnTableKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST5) key column (ID) is not the column "
            + "used in the join criteria (TEST5.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.col0;");
  }

  @Test
  public void shouldRepartitionBothStreamsIfJoiningOnRowKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.rowkey = test3.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST2.ROWTIME BIGINT"));
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST3.ROWTIME BIGINT"));
  }

  @Test
  public void shouldRepartitionBothStreamsIfJoiningOnRowKeyWhenStreamsHaveNoKeyField_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test6", "test7");
    execute(CREATE_STREAM_TEST6 + CREATE_STREAM_TEST7);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test6 JOIN test7 WITHIN 1 SECOND "
        + "ON test6.rowkey = test7.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST7.ROWTIME BIGINT"));
    assertThat(result.getExecutionPlan(),
        containsString("[ REKEY ] | Schema: [TEST7.ROWTIME BIGINT"));
  }

  @Test
  public void shouldHandleLeftTableJoiningOnRowKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.rowkey = test5.id;");

    // Then: did not throw.
  }

  @Test
  public void shouldHandleRightTableJoiningOnRowKey_Legacy() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD, true);
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.rowkey;");

    // Then: did not throw.
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromPropertiesBeforeConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true);
    givenKafkaTopicsExist("test1");
    execute(CREATE_STREAM_TEST1);

    // When:
    execute("CREATE STREAM TEST2 WITH(WRAP_SINGLE_VALUE=false) AS SELECT COL0 FROM TEST1;");

    // Then:
    assertThat(engineMetastore.getSource("TEST2"),
        hasSerdeOptions(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false);
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource("TEST5"),
        hasSerdeOptions(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldDefaultToWrappingSingleValueSchemas() {
    // Given:
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource("TEST5"),
        hasSerdeOptions(not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES))));
  }

  @SuppressWarnings("SameParameterValue")
  private void givenConfigWith(final String name, final Object value) {
    ksqlConfig = ksqlConfig.cloneWithPropertyOverwrite(ImmutableMap.of(name, value));
  }

  private List<QueryMetadata> execute(final String sql) {
    return KsqlEngineTestUtil.execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        Collections.emptyMap());
  }

  private void givenKafkaTopicsExist(final String... names) {
    Arrays.stream(names).forEach(name ->
        kafkaTopicClient.createTopic(name, 1, (short) 1, Collections.emptyMap())
    );
  }

  private static Processor getNodeByName(final QueryMetadata query, final String nodeName) {
    return (Processor) PlanTestUtil.getNodeByName(query.getTopology(), nodeName);
  }
}
