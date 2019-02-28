/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.rest.RestConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CustomExecutorsTest {

  private static final Schema SCHEMA =
      SchemaBuilder.struct().field("val", Schema.OPTIONAL_STRING_SCHEMA).build();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private KsqlConfig ksqlConfig;
  private KsqlEngine realEngine;
  private KsqlEngine engine;
  private MutableMetaStore metaStore;
  private ServiceContext serviceContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    serviceContext = TestServiceContext.create();
    engine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    realEngine = engine;

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    ksqlConfig = new KsqlConfig(configMap);
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldListKafkaTopics() {
    // Given:
    givenKsqlTopic("topic1");
    givenKafkaTopic("topic2");

    final AdminClient mockAdminClient = mock(AdminClient.class);
    final ListConsumerGroupsResult result = mock(ListConsumerGroupsResult.class);
    final KafkaFutureImpl<Collection<ConsumerGroupListing>> groups = new KafkaFutureImpl<>();

    when(result.all()).thenReturn(groups);
    when(mockAdminClient.listConsumerGroups()).thenReturn(result);
    groups.complete(ImmutableList.of());

    final ServiceContext serviceContext = TestServiceContext.create(
        this.serviceContext.getKafkaClientSupplier(),
        mockAdminClient,
        this.serviceContext.getTopicClient(),
        this.serviceContext.getSchemaRegistryClientFactory()
    );

     // When:
    final KafkaTopicsList topicsList =
        (KafkaTopicsList) CustomExecutors.LIST_TOPICS.execute(
        prepare("LIST TOPICS;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", true, ImmutableList.of(1), 0, 0),
        new KafkaTopicInfo("topic2", false, ImmutableList.of(1), 0, 0)
    ));
  }

  @Test
  public void shouldListRegisteredTopics() {
    // Given:
    final KsqlTopic topic1 = givenKsqlTopic("topic1");
    final KsqlTopic topic2 = givenKsqlTopic("topic2");

    // When:
    final KsqlTopicsList topicsList =
        (KsqlTopicsList) CustomExecutors.LIST_REGISTERED_TOPICS.execute(
        prepare("LIST REGISTERED TOPICS;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(),
        contains(new KsqlTopicInfo(topic1), new KsqlTopicInfo(topic2)));
  }

  @Test
  public void shouldListQueriesEmpty() {
    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        prepare("SHOW QUERIES;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), is(empty()));
  }

  @Test
  public void shouldListQueriesBasic() {
    // Given
    final PreparedStatement showQueries = prepare("SHOW QUERIES;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    engine = mock(KsqlEngine.class);
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), containsInAnyOrder(
        new RunningQuery(
            metadata.getStatementString(),
            metadata.getSinkNames(),
            new EntityQueryId(metadata.getQueryId()))));
  }

  @Test
  public void shouldListQueriesExtended() {
    // Given
    final PreparedStatement showQueries = prepare("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    engine = mock(KsqlEngine.class);
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueryDescriptions(), containsInAnyOrder(
        QueryDescription.forQueryMetadata(metadata)));
  }

  @Test
  public void shouldListFunctions() {
    // When:
    final FunctionNameList functionList = (FunctionNameList) CustomExecutors.LIST_FUNCTIONS.execute(
        prepare("LIST FUNCTIONS;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList.getFunctions(), hasItems(
        new SimpleFunctionInfo("EXTRACTJSONFIELD", FunctionType.scalar),
        new SimpleFunctionInfo("ARRAYCONTAINS", FunctionType.scalar),
        new SimpleFunctionInfo("CONCAT", FunctionType.scalar),
        new SimpleFunctionInfo("TOPK", FunctionType.aggregate),
        new SimpleFunctionInfo("MAX", FunctionType.aggregate)));

    assertThat("shouldn't contain internal functions", functionList.getFunctions(),
        not(hasItem(new SimpleFunctionInfo("FETCH_FIELD_FROM_STRUCT", FunctionType.scalar))));
  }

  @Test
  public void shouldListProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        prepare("LIST PROPERTIES;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(),
        equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
    assertThat(properties.getOverwrittenProperties(), is(empty()));
  }

  @Test
  public void shouldListPropertiesWithOverrides() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        prepare("LIST PROPERTIES;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of("auto.offset.reset", "latest")
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(),
        hasEntry("ksql.streams.auto.offset.reset", "latest"));
    assertThat(properties.getOverwrittenProperties(), hasItem("ksql.streams.auto.offset.reset"));
  }

  @Test
  public void shouldNotListSslProperties() {
    // When:
    final PropertiesList properties = (PropertiesList) CustomExecutors.LIST_PROPERTIES.execute(
        prepare("LIST PROPERTIES;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(properties.getProperties(), not(hasKey(isIn(KsqlConfig.SSL_CONFIG_NAMES))));
  }

  @Test
  public void shouldShowStreams() {
    // Given:
    final KsqlStream stream1 = givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream stream2 = givenSource(DataSourceType.KSTREAM, "stream2");
    givenSource(DataSourceType.KTABLE, "table");

    // When:
    final StreamsList descriptionList = (StreamsList)
        CustomExecutors.LIST_STREAMS.execute(
            prepare("SHOW STREAMS;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getStreams(), containsInAnyOrder(
        new SourceInfo.Stream(stream1),
        new SourceInfo.Stream(stream2)
    ));
  }

  @Test
  public void shouldShowStreamsExtended() {
    // Given:
    final KsqlStream stream1 = givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream stream2 = givenSource(DataSourceType.KSTREAM, "stream2");
    givenSource(DataSourceType.KTABLE, "table");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_STREAMS.execute(
            prepare("SHOW STREAMS EXTENDED;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(stream1, true, "JSON", ImmutableList.of(), ImmutableList.of(), null),
        new SourceDescription(stream2, true, "JSON", ImmutableList.of(), ImmutableList.of(), null)
    ));
  }

  @Test
  public void shouldShowTables() {
    // Given:
    final KsqlTable table1 = givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable table2 = givenSource(DataSourceType.KTABLE, "table2");
    givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final TablesList descriptionList = (TablesList)
        CustomExecutors.LIST_TABLES.execute(
            prepare("LIST TABLES;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getTables(), containsInAnyOrder(
        new SourceInfo.Table(table1),
        new SourceInfo.Table(table2)
    ));
  }

  @Test
  public void shouldShowTablesExtended() {
    // Given:
    final KsqlTable table1 = givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable table2 = givenSource(DataSourceType.KTABLE, "table2");
    givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_TABLES.execute(
            prepare("LIST TABLES EXTENDED;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    final KafkaTopicClient client = serviceContext.getTopicClient();
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(table1, true, "JSON", ImmutableList.of(), ImmutableList.of(), client),
        new SourceDescription(table2, true, "JSON", ImmutableList.of(), ImmutableList.of(), client)
    ));
  }

  @Test
  public void shouldShowColumnsSource() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "SOURCE");
    final ExecuteResult result = engine.execute(
        prepare("CREATE STREAM SINK AS SELECT * FROM source;"),
        ksqlConfig,
        ImmutableMap.of());
    final PersistentQueryMetadata metadata = (PersistentQueryMetadata) result.getQuery()
        .orElseThrow(IllegalArgumentException::new);
    final StructuredDataSource stream = engine.getMetaStore().getSource("SINK");

    // When:
    final SourceDescriptionEntity sourceDescription = (SourceDescriptionEntity)
        CustomExecutors.SHOW_COLUMNS.execute(
            PreparedStatement.of(
                "DESCRIBE SINK;",
                new ShowColumns(QualifiedName.of("SINK"), false, false)),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(sourceDescription.getSourceDescription(),
        equalTo(new SourceDescription(
            stream,
            false,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(new RunningQuery(
                metadata.getStatementString(),
                metadata.getSinkNames(),
                new EntityQueryId(metadata.getQueryId()))),
            null)));
  }

  @Test
  public void shouldShowColumnsTopic() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "S");

    // When:
    final TopicDescription description = (TopicDescription) CustomExecutors.SHOW_COLUMNS.execute(
        prepare("DESCRIBE TOPIC S;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(description,
        equalTo(new TopicDescription("DESCRIBE TOPIC S;", "S", "S", "JSON", SCHEMA.toString())));
  }

  @Test
  public void shouldThrowOnDescribeMissingTopic() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find Topic");

    // When:
    CustomExecutors.SHOW_COLUMNS.execute(
        prepare("DESCRIBE TOPIC S;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    );
  }

  @Test
  public void shouldSetProperty() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "stream");
    final Map<String, Object> properties = new HashMap<>();

    // When:
    CustomExecutors.SET_PROPERTY.execute(
        prepare("SET 'property' = 'value';"),
        engine,
        serviceContext,
        ksqlConfig,
        properties
    );

    // Then:
    assertThat(properties, hasEntry("property", "value"));
  }

  @Test
  public void shouldUnSetProperty() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "stream");
    final Map<String, Object> properties = new HashMap<>();
    properties.put("property", "value");

    // When:
    CustomExecutors.UNSET_PROPERTY.execute(
        prepare("UNSET 'property';"),
        engine,
        serviceContext,
        ksqlConfig,
        properties
    );

    // Then:
    assertThat(properties, not(hasKey("property")));
  }

  @Test
  public void shouldDescribeUDF() {
    // When:
    final FunctionDescriptionList functionList = (FunctionDescriptionList)
        CustomExecutors.DESCRIBE_FUNCTION.execute(
            prepare("DESCRIBE FUNCTION CONCAT;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList, new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(FunctionDescriptionList item) {
        return functionList.getName().equals("CONCAT")
            && functionList.getType().equals(FunctionType.scalar);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(functionList.getName());
      }
    });
  }

  @Test
  public void shouldDescribeUDAF() {
    // When:
    final FunctionDescriptionList functionList = (FunctionDescriptionList)
        CustomExecutors.DESCRIBE_FUNCTION.execute(
            prepare("DESCRIBE FUNCTION MAX;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(functionList, new TypeSafeMatcher<FunctionDescriptionList>() {
      @Override
      protected boolean matchesSafely(FunctionDescriptionList item) {
        return functionList.getName().equals("MAX")
            && functionList.getType().equals(FunctionType.aggregate);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(functionList.getName());
      }
    });
  }

  @Test
  public void shouldExplainQueryId() {
    // Given:
    final PreparedStatement explain = prepare("EXPLAIN id;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id");

    engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(metadata.getQueryId())).thenReturn(Optional.of(metadata));

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription(), equalTo(QueryDescription.forQueryMetadata(metadata)));
  }

  @Test
  public void shouldExplainPersistentStatement() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "CREATE STREAM X AS SELECT * FROM Y;";
    final PreparedStatement explain = prepare("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
    assertThat("No side effects should happen", engine.getPersistentQueries(), is(empty()));
  }

  @Test
  public void shouldExplainStatement() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "Y");
    final String statementText = "SELECT * FROM Y;";
    final PreparedStatement explain = prepare("EXPLAIN " + statementText);

    // When:
    final QueryDescriptionEntity query = (QueryDescriptionEntity) CustomExecutors.EXPLAIN.execute(
        explain,
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(query.getQueryDescription().getStatementText(), equalTo(statementText));
    assertThat(query.getQueryDescription().getSources(), containsInAnyOrder("Y"));
  }

  @Test
  public void shouldFailOnNonQueryExplain() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The provided statement does not run a ksql query");

    // When:
    CustomExecutors.EXPLAIN.execute(
        prepare("Explain SHOW TOPICS;"),
        engine,
        serviceContext,
        ksqlConfig,
        ImmutableMap.of()
    );
  }

  @SuppressWarnings("unchecked")
  private <T extends StructuredDataSource> T givenSource(
      final DataSource.DataSourceType type,
      final String name) {
    final KsqlTopic topic = givenKsqlTopic(name);
    givenKafkaTopic(name);

    final StructuredDataSource source;
    switch (type) {
      case KSTREAM:
        source =
            new KsqlStream<>(
                "statement", name, SCHEMA, SCHEMA.field("val"),
                new MetadataTimestampExtractionPolicy(), topic, Serdes.String());
        break;
      case KTABLE:
        source =
            new KsqlTable<>(
                "statement", name, SCHEMA, SCHEMA.field("val"),
                new MetadataTimestampExtractionPolicy(), topic, "store", Serdes.String());
        break;
      case KTOPIC:
      default:
        throw new IllegalArgumentException(type.toString());
    }
    metaStore.putSource(source);

    return (T) source;
  }

  private KsqlTopic givenKsqlTopic(String name) {
    final KsqlTopic topic = new KsqlTopic(name, name, new KsqlJsonTopicSerDe(), false);
    givenKafkaTopic(name);
    metaStore.putTopic(topic);
    return topic;
  }

  private void givenKafkaTopic(final String name) {
    ((FakeKafkaTopicClient) serviceContext.getTopicClient())
        .preconditionTopicExists(name, 1, (short) 1, Collections.emptyMap());
  }

  private PreparedStatement prepare(final String sql) {
    return engine.prepare(new DefaultKsqlParser().parse(sql).get(0));
  }

  @SuppressWarnings("SameParameterValue")
  private PersistentQueryMetadata givenPersistentQuery(final String id) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getSinkNames()).thenReturn(ImmutableSet.of(id));
    when(metadata.getResultSchema()).thenReturn(SCHEMA);

    return metadata;
  }

}
