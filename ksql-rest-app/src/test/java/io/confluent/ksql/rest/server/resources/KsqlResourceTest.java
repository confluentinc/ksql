package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicsList;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KsqlResourceTest {

  private static final MetaStore mockMetastore;

  static {
    mockMetastore = new MetaStoreImpl();

    Schema schema1 = SchemaBuilder.struct().field("s1_f1", Schema.BOOLEAN_SCHEMA);
    KsqlTopic ksqlTopic1 = new KsqlTopic("ksql_topic_1", "kafka_topic_1", new KsqlJsonTopicSerDe(null));
    mockMetastore.putTopic(ksqlTopic1);
    mockMetastore.putSource(new KsqlTable("test_table", schema1, schema1.field("s1_f1"), null,
                                          ksqlTopic1, "statestore", false));

    Schema schema2 = SchemaBuilder.struct().field("s2_f1", Schema.STRING_SCHEMA).field("s2_f2", Schema.INT32_SCHEMA);
    KsqlTopic ksqlTopic2 = new KsqlTopic("ksql_topic_2", "kafka_topic_2", new KsqlJsonTopicSerDe(null));
    mockMetastore.putTopic(ksqlTopic2);
    mockMetastore.putSource(new KsqlStream("test_stream", schema2, schema2.field("s2_f2"), null,
                                           ksqlTopic2));
  }

  private static class TestKsqlResource extends KsqlResource {

    public static final long DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT = 1000;

    public final KsqlEngine ksqlEngine;
    public final CommandStore commandStore;
    public final StatementExecutor statementExecutor;

    private TestKsqlResource(KsqlEngine ksqlEngine, CommandStore commandStore, StatementExecutor statementExecutor) {
      super(ksqlEngine, commandStore, statementExecutor, DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT);

      this.ksqlEngine = ksqlEngine;
      this.commandStore = commandStore;
      this.statementExecutor = statementExecutor;

      expect(ksqlEngine.getMetaStore()).andStubReturn(mockMetastore);
    }

    public TestKsqlResource() {
      this(
          mock(KsqlEngine.class),
          mock(CommandStore.class),
          mock(StatementExecutor.class)
      );
    }

    public void replayAll() {
      replay(ksqlEngine, commandStore, statementExecutor);
    }
  }

  private <R extends KsqlEntity> R makeSingleRequest(
      TestKsqlResource testResource,
      String ksqlString,
      Statement ksqlStatement,
      Map<String, Object> streamsProperties,
      Class<R> responseClass
  ) throws Exception{
    expect(testResource.ksqlEngine.getStatements(ksqlString)).andReturn(Collections.singletonList(ksqlStatement));
    expect(testResource.getStatementStrings(ksqlString)).andReturn(Collections.singletonList(ksqlString));

    testResource.replayAll();

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
    TestKsqlResource testResource = new TestKsqlResource();

    final String ksqlTopic = "foo";
    final String kafkaTopic = "bar";
    final String format = "json";

    final String ksqlString =
        String.format("REGISTER TOPIC %s WITH (kafka_topic='%s', format='%s');", ksqlTopic,
                      kafkaTopic, format);

    final Map<String, Expression> createTopicProperties = new HashMap<>();
    createTopicProperties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(kafkaTopic));
    createTopicProperties.put(DdlConfig.FORMAT_PROPERTY, new StringLiteral(format));

    final RegisterTopic ksqlStatement = new RegisterTopic(
        QualifiedName.of(ksqlTopic),
        false,
        createTopicProperties
    );

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, ksqlTopic);
    final CommandStatus commandStatus = new CommandStatus(
        CommandStatus.Status.SUCCESS,
        "Topic registered successfully"
    );
    final CommandStatusEntity expectedCommandStatusEntity =
        new CommandStatusEntity(ksqlString, commandId, commandStatus);

    final Map<String, Object> streamsProperties = Collections.emptyMap();

    @SuppressWarnings("unchecked")
    final Future<CommandStatus> mockCommandStatusFuture = mock(Future.class);
    expect(
        mockCommandStatusFuture.get(
            TestKsqlResource.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
            TimeUnit.MILLISECONDS
        )
    ).andReturn(commandStatus);
    replay(mockCommandStatusFuture);

    expect(testResource.commandStore.distributeStatement(
        ksqlString,
        ksqlStatement,
        streamsProperties)
    ).andReturn(commandId);
    expect(
        testResource.statementExecutor.registerQueuedStatement(commandId)
    ).andReturn(mockCommandStatusFuture);

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
  public void testTimeoutRegisterTopic() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();

    final String ksqlTopic = "foo";
    final String kafkaTopic = "bar";
    final String format = "json";

    final String ksqlString =
        String.format("REGISTER TOPIC %s WITH (kafka_topic='%s', format='%s');", ksqlTopic,
                      kafkaTopic, format);

    final Map<String, Expression> createTopicProperties = new HashMap<>();
    createTopicProperties.put(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(kafkaTopic));
    createTopicProperties.put(DdlConfig.FORMAT_PROPERTY, new StringLiteral(format));

    final RegisterTopic ksqlStatement = new RegisterTopic(
        QualifiedName.of(ksqlTopic),
        false,
        createTopicProperties
    );

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, ksqlTopic);
    final CommandStatus commandStatus = new CommandStatus(CommandStatus.Status.QUEUED, "Command written to topic");
    final CommandStatusEntity expectedCommandStatusEntity =
        new CommandStatusEntity(ksqlString, commandId, commandStatus);

    final Map<String, Object> streamsProperties = Collections.emptyMap();

    @SuppressWarnings("unchecked")
    final Future<CommandStatus> mockCommandStatusFuture = mock(Future.class);
    expect(mockCommandStatusFuture.get(TestKsqlResource.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS))
        .andThrow(new TimeoutException());
    replay(mockCommandStatusFuture);

    expect(
        testResource.commandStore.distributeStatement(ksqlString, ksqlStatement, streamsProperties)
    ).andReturn(commandId);
    expect(
        testResource.statementExecutor.registerQueuedStatement(commandId)
    ).andReturn(mockCommandStatusFuture);
    expect(
        testResource.statementExecutor.getStatus(commandId)).andReturn(Optional.of(commandStatus)
    );

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
    TestKsqlResource testResource = new TestKsqlResource();
    final String ksqlString = "DESCRIBE nonexistent_table;";
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of("nonexistent_table"), false);

    makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        ErrorMessageEntity.class
    );
  }

  @Test
  public void testListTopics() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();
    final String ksqlString = "LIST TOPICS;";
    final ListTopics ksqlStatement = new ListTopics(Optional.empty());

    TopicsList topicsList = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        TopicsList.class
    );

    Collection<TopicsList.TopicInfo> testTopics = topicsList.getTopics();
    Collection<TopicsList.TopicInfo> expectedTopics = testResource.ksqlEngine.getMetaStore()
        .getAllKsqlTopics().values().stream()
        .map(TopicsList.TopicInfo::new)
        .collect(Collectors.toList());

    assertEquals(expectedTopics.size(), testTopics.size());

    for (TopicsList.TopicInfo testTopic : testTopics) {
      assertTrue(expectedTopics.contains(testTopic));
    }

    for (TopicsList.TopicInfo expectedTopic : expectedTopics) {
      assertTrue(testTopics.contains(expectedTopic));
    }
  }

  @Test
  public void testShowQueries() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();
    final String ksqlString = "SHOW QUERIES;";
    final ListQueries ksqlStatement = new ListQueries(Optional.empty());
    final String mockKafkaTopic = "lol";

    final String mockQueryStatement = String.format(
        "CREATE STREAM %s AS SELECT * FROM test_stream WHERE s2_f2 > 69;",
        mockKafkaTopic
    );

    final KsqlStructuredDataOutputNode mockQueryOutputNode = mock(KsqlStructuredDataOutputNode.class);
    expect(mockQueryOutputNode.getKafkaTopicName()).andReturn(mockKafkaTopic);
    replay(mockQueryOutputNode);

    final long mockQueryId = 1;

    final PersistentQueryMetadata mockQuery = new PersistentQueryMetadata(
        mockQueryStatement,
        null,
        mockQueryOutputNode,
        "",
        mockQueryId
    );
    final Map<Long, PersistentQueryMetadata> mockQueries = Collections.singletonMap(mockQueryId, mockQuery);

    final Queries.RunningQuery expectedRunningQuery =
        new Queries.RunningQuery(mockQueryStatement, mockKafkaTopic, mockQueryId);

    expect(testResource.ksqlEngine.getPersistentQueries()).andReturn(mockQueries);

    Queries queries = makeSingleRequest(
        testResource,
        ksqlString,
        ksqlStatement,
        Collections.emptyMap(),
        Queries.class
    );
    List<Queries.RunningQuery> testQueries = queries.getQueries();

    assertEquals(1, testQueries.size());
    assertEquals(expectedRunningQuery, testQueries.get(0));
  }

  @Test
  public void testDescribeStatement() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();
    final String tableName = "test_table";
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
        new SourceDescription(ksqlString, testResource.ksqlEngine.getMetaStore().getSource(tableName));

    assertEquals(expectedDescription, testDescription);
  }

  @Test
  public void testListStreamsStatement() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();
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
        new StreamsList.StreamInfo((KsqlStream) testResource.ksqlEngine.getMetaStore().getSource("test_stream"));

    assertEquals(expectedStream, testStreams.get(0));
  }

  @Test
  public void testListTablesStatement() throws Exception {
    TestKsqlResource testResource = new TestKsqlResource();
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
        new TablesList.TableInfo((KsqlTable) testResource.ksqlEngine.getMetaStore().getSource("test_table"));

    assertEquals(expectedTable, testTables.get(0));
  }
}
