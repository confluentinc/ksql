package io.confluent.ksql.rest.server.resources;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.rest.entity.*;
import io.confluent.ksql.rest.server.FakeKafkaTopicClient;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.*;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlConfig;
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
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KsqlResourceTest {

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

    public static KsqlResource get() throws Exception {

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

      KsqlRestConfig restConfig = new KsqlRestConfig(defaultKsqlConfig);
      KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlStreamsProperties());

      KsqlEngine ksqlEngine = new KsqlEngine(ksqlConfig, new FakeKafkaTopicClient());
      CommandStore commandStore = new CommandStore("__COMMANDS_TOPIC",
          commandConsumer, commandProducer, new CommandIdAssigner(ksqlEngine.getMetaStore()));
      StatementExecutor statementExecutor = new StatementExecutor(ksqlEngine, new StatementParser(ksqlEngine));

      addTestTopicAndSources(ksqlEngine.getMetaStore());
      return new KsqlResource(ksqlEngine, commandStore, statementExecutor, DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT);
    }

    private static Properties getDefaultKsqlConfig() {
      Map<String, Object> configMap = new HashMap<>();
      configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      configMap.put("application.id", "KsqlResourceTest");
      configMap.put("commit.interval.ms", 0);
      configMap.put("cache.max.bytes.buffering", 0);
      configMap.put("auto.offset.reset", "earliest");
      configMap.put("command.topic.suffix", "commands");

      Properties properties = new Properties();
      properties.putAll(configMap);

      return properties;
    }

    private static void addTestTopicAndSources(MetaStore metaStore) {
      Schema schema1 = SchemaBuilder.struct().field("S1_F1", Schema.BOOLEAN_SCHEMA);
      KsqlTopic ksqlTopic1 = new KsqlTopic("KSQL_TOPIC_1", "KAFKA_TOPIC_1", new KsqlJsonTopicSerDe(null));
      metaStore.putTopic(ksqlTopic1);
      metaStore.putSource(new KsqlTable("TEST_TABLE", schema1, schema1.field("S1_F1"), null,
          ksqlTopic1, "statestore", false));

      Schema schema2 = SchemaBuilder.struct().field("S2_F1", Schema.STRING_SCHEMA).field("S2_F2", Schema.INT32_SCHEMA);
      KsqlTopic ksqlTopic2 = new KsqlTopic("KSQL_TOPIC_2", "KAFKA_TOPIC_2", new KsqlJsonTopicSerDe(null));
      metaStore.putTopic(ksqlTopic2);
      metaStore.putSource(new KsqlStream("TEST_STREAM", schema2, schema2.field("S2_F2"), null,
          ksqlTopic2));
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
    KsqlResource testResource = TestKsqlResourceUtil.get();

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

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, ksqlTopic);
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
    KsqlResource testResource = TestKsqlResourceUtil.get();
    final String ksqlString = "DESCRIBE nonexistent_table;";
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of("nonexistent_table"), false);

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
    KsqlResource testResource = TestKsqlResourceUtil.get();
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
    KsqlResource testResource = TestKsqlResourceUtil.get();
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
    KsqlResource testResource = TestKsqlResourceUtil.get();
    final String tableName = "TEST_TABLE";
    final String ksqlString = String.format("DESCRIBE %s;", tableName);
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of(tableName), false);

    SourceDescription testDescription = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        SourceDescription.class
    );

    SourceDescription expectedDescription =
        new SourceDescription(ksqlString, testResource.getKsqlEngine().getMetaStore().getSource(tableName));

    assertEquals(expectedDescription, testDescription);
  }

  @Test
  public void testListStreamsStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get();
    final String ksqlString = "LIST STREAMS;";
    final ListStreams ksqlStatement = new ListStreams(Optional.empty());

    StreamsList streamsList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        StreamsList.class
    );

    List<StreamsList.StreamInfo> testStreams = streamsList.getStreams();
    assertEquals(1, testStreams.size());

    StreamsList.StreamInfo expectedStream =
        new StreamsList.StreamInfo((KsqlStream) testResource.getKsqlEngine().getMetaStore().getSource("TEST_STREAM"));

    assertEquals(expectedStream, testStreams.get(0));
  }

  @Test
  public void testListTablesStatement() throws Exception {
    KsqlResource testResource = TestKsqlResourceUtil.get();
    final String ksqlString = "LIST TABLES;";
    final ListTables ksqlStatement = new ListTables(Optional.empty());

    TablesList tablesList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        TablesList.class
    );

    List<TablesList.TableInfo> testTables = tablesList.getTables();
    assertEquals(1, testTables.size());

    TablesList.TableInfo expectedTable =
        new TablesList.TableInfo((KsqlTable) testResource.getKsqlEngine().getMetaStore().getSource("TEST_TABLE"));

    assertEquals(expectedTable, testTables.get(0));
  }
}
