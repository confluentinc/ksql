/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandIdAssigner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.rest.RestConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KsqlResourceTest {
  private KsqlRestConfig ksqlRestConfig;
  private FakeKafkaTopicClient kafkaTopicClient;
  private KsqlEngine ksqlEngine;

  @Before
  public void setUp() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    registerSchema(schemaRegistryClient);
    ksqlRestConfig = new KsqlRestConfig(TestKsqlResourceUtil.getDefaultKsqlConfig());
    KsqlConfig ksqlConfig = new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties());
    kafkaTopicClient = new FakeKafkaTopicClient();
    ksqlEngine = new KsqlEngine(
        ksqlConfig, kafkaTopicClient, schemaRegistryClient, new MetaStoreImpl());
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
  }

  private static class TestCommandProducer<K, V> extends KafkaProducer<K, V> {
    public TestCommandProducer(Map configs, Serializer keySerializer, Serializer valueSerializer) {
      super(configs, keySerializer, valueSerializer);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record) {
      // Fake result: only for testing purpose
      return ConcurrentUtils.constantFuture(new RecordMetadata(null, 0, 0, 0, 0, 0, 0));
    }
  }

  private static class TestCommandConsumer<K, V> extends KafkaConsumer<K, V> {
    public TestCommandConsumer(Map configs, Deserializer keyDeserializer, Deserializer valueDeserializer) {
      super(configs, keyDeserializer, valueDeserializer);
    }
  }

  private static class TestKsqlResourceUtil {

    public static final long DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT = 1000;

    public static KsqlResource get(KsqlEngine ksqlEngine,
                                   KsqlRestConfig restConfig) throws Exception {

      Properties defaultKsqlConfig = getDefaultKsqlConfig();

      // Map<String, Object> commandConsumerProperties = config.getCommandConsumerProperties();
      KafkaConsumer<CommandId, Command> commandConsumer = new TestCommandConsumer<>(
          defaultKsqlConfig,
          getJsonDeserializer(CommandId.class, true),
          getJsonDeserializer(Command.class, false)
      );

      KafkaProducer<CommandId, Command> commandProducer = new TestCommandProducer<>(
          defaultKsqlConfig,
          getJsonSerializer(true),
          getJsonSerializer(false)
      );

      CommandStore commandStore = new CommandStore("__COMMANDS_TOPIC",
                                                   commandConsumer, commandProducer, new CommandIdAssigner(ksqlEngine.getMetaStore()));
      StatementExecutor statementExecutor = new StatementExecutor(ksqlEngine, new StatementParser(ksqlEngine));

      addTestTopicAndSources(ksqlEngine.getMetaStore(), ksqlEngine.getTopicClient());
      return new KsqlResource(ksqlEngine, commandStore, statementExecutor, DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT);
    }

    private static Properties getDefaultKsqlConfig() {
      Map<String, Object> configMap = new HashMap<>();
      configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      configMap.put("application.id", "KsqlResourceTest");
      configMap.put("commit.interval.ms", 0);
      configMap.put("cache.max.bytes.buffering", 0);
      configMap.put("auto.offset.reset", "earliest");
      configMap.put("ksql.command.topic.suffix", "commands");
      configMap.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");

      Properties properties = new Properties();
      properties.putAll(configMap);

      return properties;
    }

    private static void addTestTopicAndSources(MetaStore metaStore, KafkaTopicClient kafkaTopicClient) {
      Schema schema1 = SchemaBuilder.struct().field("S1_F1", Schema.BOOLEAN_SCHEMA);
      KsqlTopic ksqlTopic1 = new KsqlTopic("KSQL_TOPIC_1", "KAFKA_TOPIC_1", new KsqlJsonTopicSerDe());
      metaStore.putTopic(ksqlTopic1);
      metaStore.putSource(new KsqlTable("statementText", "TEST_TABLE", schema1, schema1.field("S1_F1"), null,
                                        ksqlTopic1, "statestore", false));

      Schema schema2 = SchemaBuilder.struct().field("S2_F1", Schema.STRING_SCHEMA).field("S2_F2", Schema.INT32_SCHEMA);
      KsqlTopic ksqlTopic2 = new KsqlTopic("KSQL_TOPIC_2", "KAFKA_TOPIC_2", new KsqlJsonTopicSerDe());
      metaStore.putTopic(ksqlTopic2);
      metaStore.putSource(new KsqlStream("statementText", "TEST_STREAM", schema2, schema2.field("S2_F2"), null,
                                         ksqlTopic2));
      kafkaTopicClient.createTopic("orders-topic", 1, (short)1);
    }

    private static <T> Deserializer<T> getJsonDeserializer(Class<T> classs, boolean isKey) {
      Deserializer<T> result = new KafkaJsonDeserializer<>();
      String typeConfigProperty = isKey
                                  ? KafkaJsonDeserializerConfig.JSON_KEY_TYPE
                                  : KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;

      Map<String, ?> props = Collections.singletonMap(
          typeConfigProperty,
          classs
      );
      result.configure(props, isKey);
      return result;
    }

    private static <T> Serializer<T> getJsonSerializer(boolean isKey) {
      Serializer<T> result = new KafkaJsonSerializer<>();
      result.configure(Collections.emptyMap(), isKey);
      return result;
    }

  }

  private <R extends KsqlEntity> R makeSingleRequest(
      KsqlResource testResource,
      String ksqlString,
      Statement ksqlStatement,
      Map<String, Object> streamsProperties,
      Class<R> responseClass
  ) throws Exception{

    Object responseEntity = testResource.handleKsqlStatements(
        new KsqlRequest(ksqlString, streamsProperties)
    ).getEntity();
    assertThat(responseEntity, instanceOf(List.class));

    List responseList = (List) responseEntity;
    assertEquals(1, responseList.size());

    Object responseElement = responseList.get(0);
    assertThat(responseElement, instanceOf(responseClass));

    return responseClass.cast(responseElement);
  }

  @Test
  public void testInstantRegisterTopic() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);

    final String ksqlTopic = "FOO";
    final String kafkaTopic = "bar";
    final String format = "json";

    final String ksqlString =
        String.format("REGISTER TOPIC %s WITH (kafka_topic='%s', value_format='%s');",
                      ksqlTopic,
                      kafkaTopic, format);

    final Map<String, Expression> createTopicProperties = new HashMap<>();
    createTopicProperties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(kafkaTopic));
    createTopicProperties.put(DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral(format));

    final RegisterTopic ksqlStatement = new RegisterTopic(
        QualifiedName.of(ksqlTopic),
        false,
        createTopicProperties
    );

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, ksqlTopic, CommandId.Action.CREATE);
    final CommandStatus commandStatus = new CommandStatus(
        CommandStatus.Status.QUEUED,
        "Statement written to command topic"
    );

    final CommandStatusEntity expectedCommandStatusEntity =
        new CommandStatusEntity(ksqlString, commandId, commandStatus);

    final Map<String, Object> streamsProperties = Collections.emptyMap();

    KsqlEntity testKsqlEntity = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        streamsProperties,
        KsqlEntity.class
    );

    assertEquals(expectedCommandStatusEntity, testKsqlEntity);
  }

  @Test
  public void testErroneousStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "DESCRIBE nonexistent_table;";
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of("nonexistent_table"), false, false);

    KsqlEntity resultEntity = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        ErrorMessageEntity.class
    );

    assertEquals(ErrorMessageEntity.class, resultEntity.getClass());
  }

  @Test
  public void testListRegisteredTopics() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "LIST REGISTERED TOPICS;";
    final ListRegisteredTopics ksqlStatement = new ListRegisteredTopics(Optional.empty());

    KsqlTopicsList ksqlTopicsList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        KsqlTopicsList.class
    );

    Collection<KsqlTopicInfo> testTopics = ksqlTopicsList.getTopics();
    Collection<KsqlTopicInfo> expectedTopics = testResource.getKsqlEngine().getMetaStore()
        .getAllKsqlTopics().values().stream()
        .map(KsqlTopicInfo::new)
        .collect(Collectors.toList());

    assertEquals(expectedTopics.size(), testTopics.size());

    for (KsqlTopicInfo testTopic : testTopics) {
      assertTrue(expectedTopics.contains(testTopic));
    }

    for (KsqlTopicInfo expectedTopic : expectedTopics) {
      assertTrue(testTopics.contains(expectedTopic));
    }
  }

  @Test
  public void testShowQueries() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "SHOW QUERIES;";
    final ListQueries ksqlStatement = new ListQueries(Optional.empty());
    final String testKafkaTopic = "lol";

    final String testQueryStatement = String.format(
        "CREATE STREAM %s AS SELECT * FROM test_stream WHERE S2_F2 > 69;",
        testKafkaTopic
    );

    Queries queries = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        Queries.class
    );
    List<Queries.RunningQuery> testQueries = queries.getQueries();

    assertEquals(0, testQueries.size());
  }

  @Test
  public void testDescribeStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String tableName = "TEST_TABLE";
    final String ksqlString = String.format("DESCRIBE %s;", tableName);
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of(tableName), false, false);

    SourceDescription testDescription = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        SourceDescription.class
    );

    SourceDescription expectedDescription =
        new SourceDescription(testResource.getKsqlEngine().getMetaStore().getSource(tableName), false, "serdes", "topo", "exec-plan", Collections.EMPTY_LIST, Collections.EMPTY_LIST,null);

    assertEquals(expectedDescription, testDescription);
  }

  @Test
  public void testListStreamsStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "LIST STREAMS;";
    final ListStreams ksqlStatement = new ListStreams(Optional.empty());

    StreamsList streamsList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        StreamsList.class
    );

    List<SourceInfo.Stream> testStreams = streamsList.getStreams();
    assertEquals(1, testStreams.size());

    SourceInfo expectedStream =
        new SourceInfo.Stream((KsqlStream) testResource.getKsqlEngine().getMetaStore().getSource("TEST_STREAM"));

    assertEquals(expectedStream, testStreams.get(0));
  }

  @Test
  public void testListTablesStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "LIST TABLES;";
    final ListTables ksqlStatement = new ListTables(Optional.empty());

    TablesList tablesList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        TablesList.class
    );

    List<SourceInfo.Table> testTables = tablesList.getTables();
    assertEquals(1, testTables.size());

    SourceInfo expectedTable =
        new SourceInfo.Table((KsqlTable) testResource.getKsqlEngine().getMetaStore().getSource("TEST_TABLE"));

    assertEquals(expectedTable, testTables.get(0));
  }

  @Test
  public void shouldFailForIncorrectCSASStatementResultType() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    String ksqlString1 = "CREATE STREAM s1 AS SELECT * FROM test_table;";

    Response response1 = testResource.handleKsqlStatements(new KsqlRequest(ksqlString1, Collections
        .emptyMap()));
    KsqlEntityList result1 = (KsqlEntityList) response1.getEntity();
    assertTrue("Incorrect response size.", result1.size() == 1);
    assertThat(result1.get(0), instanceOf(ErrorMessageEntity.class));
    ErrorMessageEntity errorMessageEntity1 = (ErrorMessageEntity) result1.get(0);
    assertThat("", errorMessageEntity1.getErrorMessage().getMessage(), equalTo("Invalid result type. Your SELECT query produces a TABLE. Please use CREATE TABLE AS SELECT statement instead."));

    String ksqlString2 = "CREATE STREAM s2 AS SELECT S2_F1 , count(S2_F1) FROM test_stream group by "
                         + "s2_f1;";

    Response response2 = testResource.handleKsqlStatements(new KsqlRequest(ksqlString2, Collections
        .emptyMap()));
    KsqlEntityList result2 = (KsqlEntityList) response2.getEntity();
    assertThat("Incorrect response size.", result2.size(),  equalTo(1));
    assertThat(result2.get(0), instanceOf(ErrorMessageEntity.class));
    ErrorMessageEntity errorMessageEntity2 = (ErrorMessageEntity) result2.get(0);
    assertThat("", errorMessageEntity2.getErrorMessage().getMessage(), equalTo("Invalid "
                                                                               + "result type. Your SELECT query produces a TABLE. Please use CREATE TABLE AS SELECT statement instead."));
  }

  @Test
  public void shouldFailForIncorrectCTASStatementResultType() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "CREATE TABLE s1 AS SELECT * FROM test_stream;";

    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, Collections
        .emptyMap()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Incorrect response size.", result.size(), equalTo(1));
    assertThat(result.get(0), instanceOf(ErrorMessageEntity.class));
    ErrorMessageEntity errorMessageEntity = (ErrorMessageEntity) result.get(0);
    assertTrue(errorMessageEntity.getErrorMessage().getMessage().contains("Invalid result type. Your "
                                                                          + "SELECT query produces a STREAM. Please use CREATE STREAM AS SELECT statement instead."));
  }

  @Test
  public void shouldFailForIncorrectDropStreamStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "DROP TABLE test_stream;";
    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, Collections
        .emptyMap()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Incorrect drop statement.", result.size(), equalTo(1));
    assertThat(result.get(0), instanceOf(ErrorMessageEntity.class));
    ErrorMessageEntity errorMessageEntity = (ErrorMessageEntity) result.get(0);
    assertTrue(errorMessageEntity.getErrorMessage().getMessage().equalsIgnoreCase("Incompatible data source type"
                                                                                  + " is STREAM, but statement was DROP TABLE"));
  }

  @Test
  public void shouldFailForIncorrectDropTableStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "DROP STREAM test_table;";
    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, Collections
        .emptyMap()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Incorrect drop statement.", result.size(), equalTo(1));
    assertThat(result.get(0), instanceOf(ErrorMessageEntity.class));
    ErrorMessageEntity errorMessageEntity = (ErrorMessageEntity) result.get(0);
    assertTrue(errorMessageEntity.getErrorMessage().getMessage().equalsIgnoreCase("Incompatible data source type"
                                                                                  + " is TABLE, but statement was DROP STREAM"));
  }

  @Test
  public void shouldCreateTableWithInference() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "CREATE TABLE orders WITH (KAFKA_TOPIC='orders-topic', "
                              + "VALUE_FORMAT = 'avro', KEY = 'orderid');";

    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, new HashMap<>()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Incorrect response.", result.size(), equalTo(1));
    assertThat(result.get(0), instanceOf(CommandStatusEntity.class));
    CommandStatusEntity commandStatusEntity = (CommandStatusEntity) result.get(0);
    assertThat(
        commandStatusEntity.getCommandId().getType().name().toUpperCase(),
        equalTo("TABLE"));
  }

  @Test
  public void shouldFailCreateTableWithInferenceWithIncorrectKey() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlString = "CREATE TABLE orders WITH (KAFKA_TOPIC='orders-topic', "
                              + "VALUE_FORMAT = 'avro', KEY = 'orderid1');";

    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, new HashMap<>()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Incorrect response.", result.size(), equalTo(1));
    assertThat(result.get(0), instanceOf(ErrorMessageEntity.class));
  }

  private void validateQueryDescription(
      String ksqlQueryString,
      Map<String, Object> overriddenProperties,
      KsqlEntity entity) throws Exception {
    assertThat(entity, instanceOf(SourceDescription.class));
    QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(
        ksqlQueryString, overriddenProperties).get(0);
    validateQueryDescription(queryMetadata, overriddenProperties, entity);
  }

  private void validateQueryDescription(
      QueryMetadata queryMetadata,
      Map<String, Object> overriddenProperties,
      KsqlEntity entity) {
    SourceDescription sourceDescription = (SourceDescription) entity;
    assertThat(sourceDescription.getType(), equalTo("QUERY"));
    assertThat(sourceDescription.getExecutionPlan(), equalTo(queryMetadata.getExecutionPlan()));
    assertThat(sourceDescription.getTopology(), equalTo(queryMetadata.getTopologyDescription()));
    assertThat(
        sourceDescription.getSchema(),
        equalTo(
            queryMetadata.getOutputNode().getSchema().fields()
                .stream()
                .map(f -> new SourceDescription.FieldSchemaInfo(
                    f.name(), SchemaUtil.getSchemaFieldName(f)))
                .collect(Collectors.toList())));
    assertThat(
        sourceDescription.getOverriddenProperties(),
        equalTo(overriddenProperties));
  }

  @Test
  public void shouldFillExplainQueryWithCorrectInfo() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);
    final String ksqlQueryString = "SELECT * FROM test_stream;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;
    Response response = testResource.handleKsqlStatements(
        new KsqlRequest(ksqlString, new HashMap<>()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Response should have 1 entity", result.size(), equalTo(1));
    validateQueryDescription(
        ksqlQueryString,
        Collections.emptyMap(),
        result.get(0));
  }

  @Test
  public void shouldFillExplainQueryByIDWithCorrectInfo() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get(ksqlEngine, ksqlRestConfig);

    final String ksqlQueryString = "CREATE STREAM test_explain AS SELECT * FROM test_stream;";
    Map<String, Object> overriddenProperties =
        Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest");
    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(
            ksqlQueryString,
            overriddenProperties).get(0);

    final String ksqlString = "EXPLAIN " + queryMetadata.getId() + ";";
    Response response = testResource.handleKsqlStatements(new KsqlRequest(ksqlString, new HashMap<>()));
    KsqlEntityList result = (KsqlEntityList) response.getEntity();
    assertThat("Response should have 1 entity", result.size(), equalTo(1));
    validateQueryDescription(queryMetadata, overriddenProperties, result.get(0));
  }

  private void registerSchema(SchemaRegistryClient schemaRegistryClient)
      throws IOException, RestClientException {
    String ordersAveroSchemaStr = "{"
                                  + "\"namespace\": \"kql\","
                                  + " \"name\": \"orders\","
                                  + " \"type\": \"record\","
                                  + " \"fields\": ["
                                  + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
                                  + "     {\"name\": \"orderid\",  \"type\": \"long\"},"
                                  + "     {\"name\": \"itemid\", \"type\": \"string\"},"
                                  + "     {\"name\": \"orderunits\", \"type\": \"double\"},"
                                  + "     {\"name\": \"arraycol\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},"
                                  + "     {\"name\": \"mapcol\", \"type\": {\"type\": \"map\", \"values\": \"double\"}}"
                                  + " ]"
                                  + "}";
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(ordersAveroSchemaStr);
    schemaRegistryClient.register("orders-topic" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX,
                                  avroSchema);

  }

}
