package io.confluent.kql.rest.server.resources;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.ListStreams;
import io.confluent.kql.parser.tree.ListTables;
import io.confluent.kql.parser.tree.ListTopics;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.ShowColumns;
import io.confluent.kql.parser.tree.ShowQueries;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.StringLiteral;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.rest.server.computation.CommandId;
import io.confluent.kql.rest.server.computation.CommandStore;
import io.confluent.kql.rest.server.computation.StatementExecutor;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.util.PersistentQueryMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class KQLResourceTest {

  private static final MetaStore mockMetastore;

  static {
    mockMetastore = new MetaStoreImpl();

    Schema schema1 = SchemaBuilder.struct().field("s1_f1", Schema.BOOLEAN_SCHEMA);
    KQLTopic kqlTopic1 = new KQLTopic("kql_topic_1", "kafka_topic_1", new KQLJsonTopicSerDe(schema1));
    mockMetastore.putTopic(kqlTopic1);
    mockMetastore.putSource(new KQLTable("test_table", schema1, schema1.field("s1_f1"), kqlTopic1, "statestore", false));

    Schema schema2 = SchemaBuilder.struct().field("s2_f1", Schema.STRING_SCHEMA).field("s2_f2", Schema.INT32_SCHEMA);
    KQLTopic kqlTopic2 = new KQLTopic("kql_topic_2", "kafka_topic_2", new KQLJsonTopicSerDe(schema2));
    mockMetastore.putTopic(kqlTopic2);
    mockMetastore.putSource(new KQLStream("test_stream", schema2, schema2.field("s2_f2"), kqlTopic2));
  }

  private static class TestKqlResource extends KQLResource {

    public final KQLEngine kqlEngine;
    public final CommandStore commandStore;
    public final StatementExecutor statementExecutor;

    private TestKqlResource(KQLEngine kqlEngine, CommandStore commandStore, StatementExecutor statementExecutor) {
      super(kqlEngine, commandStore, statementExecutor);

      this.kqlEngine = kqlEngine;
      this.commandStore = commandStore;
      this.statementExecutor = statementExecutor;

      expect(kqlEngine.getMetaStore()).andStubReturn(mockMetastore);
    }

    public TestKqlResource() {
      this(
          mock(KQLEngine.class),
          mock(CommandStore.class),
          mock(StatementExecutor.class)
      );
    }

    public void replayAll() {
      replay(kqlEngine);
      replay(commandStore);
      replay(statementExecutor);
    }
  }

  private static KQLJsonRequest createJsonRequest(String kql) {
    KQLJsonRequest result = new KQLJsonRequest();
    result.kql = kql;
    return result;
  }

  private JsonValue makeSingleRequest(
      TestKqlResource testResource,
      String kqlString,
      Statement kqlStatement,
      String responseField
  ) throws Exception{
    expect(testResource.kqlEngine.getStatements(kqlString)).andReturn(Collections.singletonList(kqlStatement));
    expect(testResource.getStatementStrings(kqlString)).andReturn(Collections.singletonList(kqlString));

    testResource.replayAll();

    String responseEntity = (String) testResource.handleKQLStatements(createJsonRequest(kqlString)).getEntity();
    JsonArray jsonResponse = Json.createReader(new ByteArrayInputStream(responseEntity.getBytes())).readArray();

    assertEquals(1, jsonResponse.size());

    JsonValue responseElement = jsonResponse.get(0);
    assertThat(responseElement, instanceOf(JsonObject.class));

    JsonObject responseObject = (JsonObject) responseElement;
    assertEquals(1, responseObject.size());

    assertTrue(responseObject.containsKey(responseField));
    return responseObject.get(responseField);
  }

  @Test
  public void testCreateTopic() throws Exception {
    TestKqlResource testResource = new TestKqlResource();

    final String kqlTopic = "foo";
    final String kafkaTopic = "bar";
    final String format = "json";

    final String kqlString =
        String.format("CREATE TOPIC %s WITH (kafka_topic='%s', format='%s');", kqlTopic, kafkaTopic, format);

    final Map<String, Expression> createTopicProperties = new HashMap<>();
    createTopicProperties.put(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(kafkaTopic));
    createTopicProperties.put(DDLConfig.FORMAT_PROPERTY, new StringLiteral(format));

    final CreateTopic kqlStatement = new CreateTopic(
        QualifiedName.of(kqlTopic),
        false,
        createTopicProperties
    );

    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, kqlTopic);

    expect(testResource.commandStore.distributeStatement(kqlString, kqlStatement)).andReturn(commandId);

    testResource.statementExecutor.registerQueuedStatement(commandId);
    expectLastCall();

    JsonValue statementIdElement = makeSingleRequest(testResource, kqlString, kqlStatement, "statement_id");
    assertThat(statementIdElement, instanceOf(JsonString.class));

    JsonString statementIdString = (JsonString) statementIdElement;
    assertEquals(commandId.toString(), statementIdString.getString());
  }

  @Test
  public void testErroneousStatement() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "DESCRIBE nonexistent_table;";
    final ShowColumns kqlStatement = new ShowColumns(QualifiedName.of("nonexistent_table"));

    JsonValue errorElement = makeSingleRequest(testResource, kqlString, kqlStatement, "error");
    assertThat(errorElement, instanceOf(JsonObject.class));

    JsonObject errorObject = (JsonObject) errorElement;
    assertTrue(errorObject.containsKey("stack_trace"));
  }

  @Test
  public void testListTopic() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "LIST TOPICS;";
    final ListTopics kqlStatement = new ListTopics(Optional.empty());

    JsonValue topicsElement = makeSingleRequest(testResource, kqlString, kqlStatement, "topics");
    assertThat(topicsElement, instanceOf(JsonObject.class));

    JsonObject topicsObject = (JsonObject) topicsElement;
    assertEquals(mockMetastore.getAllKQLTopics().size(), topicsObject.size());
    assertEquals(mockMetastore.getAllTopicNames(), topicsObject.keySet());
  }

  @Test
  public void testShowQueries() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "SHOW QUERIES;";
    final ShowQueries kqlStatement = new ShowQueries(Optional.empty());

    final String mockQueryStatement = "CREATE STREAM lol AS SELECT * FROM test_stream WHERE s2_f2 > 69;";

    final KQLStructuredDataOutputNode mockQueryOutputNode = mock(KQLStructuredDataOutputNode.class);
    expect(mockQueryOutputNode.getKafkaTopicName()).andReturn("lol");
    replay(mockQueryOutputNode);

    final long mockQueryId = 1;

    final PersistentQueryMetadata mockQuery = new PersistentQueryMetadata(
        mockQueryStatement,
        null,
        mockQueryOutputNode,
        mockQueryId
    );
    final Map<Long, PersistentQueryMetadata> mockQueries = Collections.singletonMap(mockQueryId, mockQuery);

    expect(testResource.kqlEngine.getPersistentQueries()).andReturn(mockQueries);

    JsonValue queriesElement = makeSingleRequest(testResource, kqlString, kqlStatement, "queries");
    assertThat(queriesElement, instanceOf(JsonObject.class));

    JsonObject queriesObject = (JsonObject) queriesElement;
    assertEquals(1, queriesObject.size());
    assertTrue(queriesObject.containsKey(Long.toString(mockQueryId)));
  }

  @Test
  public void testDescribeStatement() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "DESCRIBE test_table;";
    final ShowColumns kqlStatement = new ShowColumns(QualifiedName.of("test_table"));

    makeSingleRequest(testResource, kqlString, kqlStatement, "description");
  }

  @Test
  public void testListStreamsStatement() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "LIST STREAMS;";
    final ListStreams kqlStatement = new ListStreams(Optional.empty());

    JsonValue streamsElement = makeSingleRequest(testResource, kqlString, kqlStatement, "streams");
    assertThat(streamsElement, instanceOf(JsonObject.class));

    JsonObject streamsObject = (JsonObject) streamsElement;
    assertTrue(streamsObject.containsKey("test_stream"));
  }

  @Test
  public void testListTablesStatement() throws Exception {
    TestKqlResource testResource = new TestKqlResource();
    final String kqlString = "LIST TABLES;";
    final ListTables kqlStatement = new ListTables(Optional.empty());

    JsonValue tablesElement = makeSingleRequest(testResource, kqlString, kqlStatement, "tables");
    assertThat(tablesElement, instanceOf(JsonObject.class));

    JsonObject streamsObject = (JsonObject) tablesElement;
    assertTrue(streamsObject.containsKey("test_table"));
  }

}
