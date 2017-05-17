package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.ddl.DDLConfig;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.planner.plan.KSQLStructuredDataOutputNode;
import io.confluent.ksql.rest.entity.CommandIdEntity;
import io.confluent.ksql.rest.entity.KSQLEntity;
import io.confluent.ksql.rest.entity.KSQLError;
import io.confluent.ksql.rest.entity.KSQLRequest;
import io.confluent.ksql.rest.entity.RunningQueries;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicsList;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
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
import java.util.stream.Collectors;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KSQLResourceTest {

  private static final MetaStore mockMetastore;

  static {
    mockMetastore = new MetaStoreImpl();

    Schema schema1 = SchemaBuilder.struct().field("s1_f1", Schema.BOOLEAN_SCHEMA);
    KSQLTopic ksqlTopic1 = new KSQLTopic("ksql_topic_1", "kafka_topic_1", new KSQLJsonTopicSerDe());
    mockMetastore.putTopic(ksqlTopic1);
    mockMetastore.putSource(new KSQLTable("test_table", schema1, schema1.field("s1_f1"), ksqlTopic1, "statestore", false));

    Schema schema2 = SchemaBuilder.struct().field("s2_f1", Schema.STRING_SCHEMA).field("s2_f2", Schema.INT32_SCHEMA);
    KSQLTopic ksqlTopic2 = new KSQLTopic("ksql_topic_2", "kafka_topic_2", new KSQLJsonTopicSerDe());
    mockMetastore.putTopic(ksqlTopic2);
    mockMetastore.putSource(new KSQLStream("test_stream", schema2, schema2.field("s2_f2"), ksqlTopic2));
  }

  private static class TestKSQLResource extends KSQLResource {

    public final KSQLEngine ksqlEngine;
    public final CommandStore commandStore;
    public final StatementExecutor statementExecutor;

    private TestKSQLResource(KSQLEngine ksqlEngine, CommandStore commandStore, StatementExecutor statementExecutor) {
      super(ksqlEngine, commandStore, statementExecutor);

      this.ksqlEngine = ksqlEngine;
      this.commandStore = commandStore;
      this.statementExecutor = statementExecutor;

      expect(ksqlEngine.getMetaStore()).andStubReturn(mockMetastore);
    }

    public TestKSQLResource() {
      this(
          mock(KSQLEngine.class),
          mock(CommandStore.class),
          mock(StatementExecutor.class)
      );
    }

    public void replayAll() {
      replay(ksqlEngine, commandStore, statementExecutor);
    }
  }

  private <R extends KSQLEntity> R makeSingleRequest(
      TestKSQLResource testResource,
      String ksqlString,
      Statement ksqlStatement,
      Class<R> responseClass
  ) throws Exception{
    expect(testResource.ksqlEngine.getStatements(ksqlString)).andReturn(Collections.singletonList(ksqlStatement));
    expect(testResource.getStatementStrings(ksqlString)).andReturn(Collections.singletonList(ksqlString));

    testResource.replayAll();

    Object responseEntity = testResource.handleKSQLStatements(new KSQLRequest(ksqlString)).getEntity();
    assertThat(responseEntity, instanceOf(List.class));

    List responseList = (List) responseEntity;
    assertEquals(1, responseList.size());

    Object responseElement = responseList.get(0);
    assertThat(responseElement, instanceOf(responseClass));

    return responseClass.cast(responseElement);
  }

  @Test
  public void testCreateTopic() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();

    final String ksqlTopic = "foo";
    final String kafkaTopic = "bar";
    final String format = "json";

    final String ksqlString =
        String.format("CREATE TOPIC %s WITH (kafka_topic='%s', format='%s');", ksqlTopic, kafkaTopic, format);

    final Map<String, Expression> createTopicProperties = new HashMap<>();
    createTopicProperties.put(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(kafkaTopic));
    createTopicProperties.put(DDLConfig.FORMAT_PROPERTY, new StringLiteral(format));

    final CreateTopic ksqlStatement = new CreateTopic(
        QualifiedName.of(ksqlTopic),
        false,
        createTopicProperties
    );

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, ksqlTopic);

    expect(testResource.commandStore.distributeStatement(ksqlString, ksqlStatement)).andReturn(commandId);

    testResource.statementExecutor.registerQueuedStatement(commandId);
    expectLastCall();

    CommandIdEntity commandIdEntity =
        makeSingleRequest(testResource, ksqlString, ksqlStatement, CommandIdEntity.class);

    assertEquals(commandId, commandIdEntity.getCommandId());
  }

  @Test
  public void testErroneousStatement() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();
    final String ksqlString = "DESCRIBE nonexistent_table;";
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of("nonexistent_table"));

    makeSingleRequest(testResource, ksqlString, ksqlStatement, KSQLError.class);
  }

  @Test
  public void testListTopics() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();
    final String ksqlString = "LIST TOPICS;";
    final ListTopics ksqlStatement = new ListTopics(Optional.empty());

    TopicsList topicsList = makeSingleRequest(testResource, ksqlString, ksqlStatement, TopicsList.class);

    Collection<TopicsList.TopicInfo> testTopics = topicsList.getTopics();
    Collection<TopicsList.TopicInfo> expectedTopics = testResource.ksqlEngine.getMetaStore()
        .getAllKSQLTopics().values().stream()
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
    TestKSQLResource testResource = new TestKSQLResource();
    final String ksqlString = "SHOW QUERIES;";
    final ListQueries ksqlStatement = new ListQueries(Optional.empty());
    final String mockKafkaTopic = "lol";

    final String mockQueryStatement = String.format(
        "CREATE STREAM %s AS SELECT * FROM test_stream WHERE s2_f2 > 69;",
        mockKafkaTopic
    );

    final KSQLStructuredDataOutputNode mockQueryOutputNode = mock(KSQLStructuredDataOutputNode.class);
    expect(mockQueryOutputNode.getKafkaTopicName()).andReturn(mockKafkaTopic);
    replay(mockQueryOutputNode);

    final long mockQueryId = 1;

    final PersistentQueryMetadata mockQuery = new PersistentQueryMetadata(
        mockQueryStatement,
        null,
        mockQueryOutputNode,
        mockQueryId
    );
    final Map<Long, PersistentQueryMetadata> mockQueries = Collections.singletonMap(mockQueryId, mockQuery);

    final RunningQueries.RunningQuery expectedRunningQuery =
        new RunningQueries.RunningQuery(mockQueryStatement, mockKafkaTopic, mockQueryId);

    expect(testResource.ksqlEngine.getPersistentQueries()).andReturn(mockQueries);

    RunningQueries runningQueries = makeSingleRequest(testResource, ksqlString, ksqlStatement, RunningQueries.class);
    List<RunningQueries.RunningQuery> testQueries = runningQueries.getRunningQueries();

    assertEquals(1, testQueries.size());
    assertEquals(expectedRunningQuery, testQueries.get(0));
  }

  @Test
  public void testDescribeStatement() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();
    final String tableName = "test_table";
    final String ksqlString = String.format("DESCRIBE %s;", tableName);
    final ShowColumns ksqlStatement = new ShowColumns(QualifiedName.of(tableName));

    SourceDescription testDescription =
        makeSingleRequest(testResource, ksqlString, ksqlStatement, SourceDescription.class);

    SourceDescription expectedDescription =
        new SourceDescription(ksqlString, testResource.ksqlEngine.getMetaStore().getSource(tableName));

    assertEquals(expectedDescription, testDescription);
  }

  @Test
  public void testListStreamsStatement() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();
    final String ksqlString = "LIST STREAMS;";
    final ListStreams ksqlStatement = new ListStreams(Optional.empty());

    StreamsList streamsList = makeSingleRequest(testResource, ksqlString, ksqlStatement, StreamsList.class);

    List<StreamsList.StreamInfo> testStreams = streamsList.getStreams();
    assertEquals(1, testStreams.size());

    StreamsList.StreamInfo expectedStream =
        new StreamsList.StreamInfo((KSQLStream) testResource.ksqlEngine.getMetaStore().getSource("test_stream"));

    assertEquals(expectedStream, testStreams.get(0));
  }

  @Test
  public void testListTablesStatement() throws Exception {
    TestKSQLResource testResource = new TestKSQLResource();
    final String ksqlString = "LIST TABLES;";
    final ListTables ksqlStatement = new ListTables(Optional.empty());

    TablesList tablesList = makeSingleRequest(testResource, ksqlString, ksqlStatement, TablesList.class);

    List<TablesList.TableInfo> testTables = tablesList.getTables();
    assertEquals(1, testTables.size());

    TablesList.TableInfo expectedTable =
        new TablesList.TableInfo((KSQLTable) testResource.ksqlEngine.getMetaStore().getSource("test_table"));

    assertEquals(expectedTable, testTables.get(0));
  }
}
