/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.topic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.FakeKafkaTopicClient.FakeTopic;
import io.confluent.ksql.util.KafkaTopicClient.TopicCleanupPolicy;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTopicInjectorTest {

  private static final Schema SCHEMA = SchemaBuilder
      .struct()
      .field("F1", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  @Mock public TopicProperties.Builder builder;

  private KsqlParser parser;
  private MutableMetaStore metaStore;
  private DefaultTopicInjector injector;
  private Map<String, Object> overrides;
  private ConfiguredStatement<CreateAsSelect> statement;
  private KsqlConfig config;
  private TopicDescription sourceDescription;
  private FakeKafkaTopicClient topicClient;

  @Before
  public void setUp() {
    parser = new DefaultKsqlParser();
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    overrides = new HashMap<>();
    config = new KsqlConfig(new HashMap<>());

    topicClient = new FakeKafkaTopicClient();
    injector = new DefaultTopicInjector(topicClient, metaStore);

    topicClient.createTopic("source", 1, (short) 1);
    sourceDescription = topicClient.describeTopic("source");

    topicClient.createTopic("jSource", 2, (short) 2);

    final KsqlTopic sourceTopic =
        new KsqlTopic("SOURCE", "source", new KsqlJsonTopicSerDe(), false);
    final KsqlStream source = new KsqlStream<>(
        "",
        "SOURCE",
        SCHEMA,
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        sourceTopic,
        Serdes::String);
    metaStore.putSource(source);

    final KsqlTopic joinTopic =
        new KsqlTopic("J_SOURCE", "jSource", new KsqlJsonTopicSerDe(), false);
    final KsqlStream joinSource = new KsqlStream<>(
        "",
        "J_SOURCE",
        SCHEMA,
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        joinTopic,
        Serdes::String);
    metaStore.putSource(joinSource);

    when(builder.withName(any())).thenReturn(builder);
    when(builder.withWithClause(any())).thenReturn(builder);
    when(builder.withOverrides(any())).thenReturn(builder);
    when(builder.withKsqlConfig(any())).thenReturn(builder);
    when(builder.withSource(any())).thenReturn(builder);
    when(builder.build()).thenReturn(new TopicProperties("name", 1, (short) 1));
  }

  @Test
  public void shouldDoNothingForNonCAS() {
    // Given:
    final ConfiguredStatement<?> statement = givenStatement("LIST PROPERTIES;");

    // When:
    final ConfiguredStatement<?> result = injector.inject(statement);

    // Then:
    assertThat(result, is(sameInstance(statement)));
  }

  @Test
  public void shouldGenerateName() {
    // Given:
    givenStatement("CREATE STREAM x AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withName("X");
  }

  @Test
  public void shouldGenerateNameWithCorrectPrefixFromOverrides() {
    // Given:
    givenStatement("CREATE STREAM x AS SELECT * FROM SOURCE;");
    overrides.put(KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG, "prefix-");
    config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG, "nope"
    ));

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withName("prefix-X");
  }

  @Test
  public void shouldGenerateNameWithCorrectPrefixFromConfig() {
    // Given:
    givenStatement("CREATE STREAM x AS SELECT * FROM SOURCE;");
    config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_OUTPUT_TOPIC_NAME_PREFIX_CONFIG, "prefix-"
    ));

    // When:
    injector.inject(statement.withConfig(config), builder);

    // Then:
    verify(builder).withName("prefix-X");
  }

  @Test
  public void shouldPassThroughWithClauseToBuilder() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withWithClause(statement.getStatement().getProperties());
  }

  @Test
  public void shouldPassThroughOverridesToBuilder() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withOverrides(overrides);
  }

  @Test
  public void shouldPassThroughConfigToBuilder() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withKsqlConfig(config);
  }

  @Test
  public void shouldIdentifyAndUseCorrectSource() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withSource(argThat(supplierThatGets(sourceDescription)));
  }

  @Test
  public void shouldIdentifyAndUseCorrectSourceInJoin() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE "
        + "JOIN J_SOURCE ON SOURCE.X = J_SOURCE.X;");

    // When:
    injector.inject(statement, builder);

    // Then:
    verify(builder).withSource(argThat(supplierThatGets(sourceDescription)));
  }

  @Test
  public void shouldBuildWithClauseWithTopicProperties() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");
    when(builder.build()).thenReturn(new TopicProperties("expectedName", 10, (short) 10));

    // When:
    final ConfiguredStatement<CreateAsSelect> result = injector.inject(statement, builder);

    // Then:
    assertThat(result.getStatement().getProperties(),
        hasEntry(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("expectedName")));
    assertThat(result.getStatement().getProperties(),
        hasEntry(KsqlConstants.SINK_NUMBER_OF_PARTITIONS, new IntegerLiteral(10)));
    assertThat(result.getStatement().getProperties(),
        hasEntry(KsqlConstants.SINK_NUMBER_OF_REPLICAS, new IntegerLiteral(10)));
  }

  @Test
  public void shouldUpdateStatementText() {
    // Given:
    givenStatement("CREATE STREAM x AS SELECT * FROM SOURCE;");

    // When:
    final ConfiguredStatement<?> result = injector.inject(statement, builder);

    // Then:
    assertThat(result.getStatementText(),
        equalTo(
            "CREATE STREAM X WITH (REPLICAS = 1, PARTITIONS = 1, KAFKA_TOPIC = 'name') AS SELECT *"
                + "\nFROM SOURCE SOURCE;"));
  }

  @Test
  public void shouldCreateMissingTopic() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");
    when(builder.build()).thenReturn(new TopicProperties("expectedName", 10, (short) 10));
    assertThat("topic did not exist", !topicClient.isTopicExists("expectedName"));

    // When:
    injector.inject(statement, builder);

    // Then:
    assertThat(topicClient.createdTopics(),
        hasEntry(
            "expectedName",
            new FakeTopic("expectedName", 10, (short) 10, TopicCleanupPolicy.DELETE)));
  }

  @Test
  public void shouldCreateMissingTopicWithDeleteCleanupPolicyForStream() {
    // Given:
    givenStatement("CREATE STREAM x WITH (kafka_topic='topic') AS SELECT * FROM SOURCE;");
    when(builder.build()).thenReturn(new TopicProperties("expectedName", 10, (short) 10));

    // When:
    injector.inject(statement, builder);

    // Then:
    assertThat(topicClient.getTopicCleanupPolicy("expectedName"),
        equalTo(TopicCleanupPolicy.DELETE));
  }

  @Test
  public void shouldCreateMissingTopicWithCompactCleanupPolicyForNonWindowedTables() {
    // Given:
    givenStatement("CREATE TABLE x WITH (kafka_topic='topic') "
        + "AS SELECT * FROM SOURCE;");
    when(builder.build()).thenReturn(new TopicProperties("expectedName", 10, (short) 10));

    // When:
    injector.inject(statement, builder);

    // Then:
    assertThat(topicClient.getTopicCleanupPolicy("expectedName"),
        equalTo(TopicCleanupPolicy.COMPACT));
  }

  @Test
  public void shouldCreateMissingTopicWithDeleteCleanupPolicyForWindowedTables() {
    // Given:
    givenStatement("CREATE TABLE x WITH (kafka_topic='topic') "
        + "AS SELECT * FROM SOURCE WINDOW TUMBLING (SIZE 10 SECONDS);");
    when(builder.build()).thenReturn(new TopicProperties("expectedName", 10, (short) 10));

    // When:
    injector.inject(statement, builder);

    // Then:
    assertThat(topicClient.getTopicCleanupPolicy("expectedName"),
        equalTo(TopicCleanupPolicy.DELETE));
  }

  @SuppressWarnings("unchecked")
  private ConfiguredStatement<?> givenStatement(final String sql) {
    final PreparedStatement<?> preparedStatement =
        parser.prepare(parser.parse(sql).get(0), metaStore);
    final ConfiguredStatement<?> configuredStatement =
        ConfiguredStatement.of(
            preparedStatement,
            overrides,
            config);
    if (preparedStatement.getStatement() instanceof CreateAsSelect) {
      statement = (ConfiguredStatement<CreateAsSelect>) configuredStatement;
    }
    return configuredStatement;
  }

  private static TypeSafeMatcher<Supplier<TopicDescription>> supplierThatGets(
      final TopicDescription topicDescription) {
    return new TypeSafeMatcher<Supplier<TopicDescription>>() {
      @Override
      protected boolean matchesSafely(final Supplier<TopicDescription> item) {
        return item.get().equals(topicDescription);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(topicDescription.toString());
      }
    };
  }

}
