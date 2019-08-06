/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceTopicsExtractorTest {
  private static final LogicalSchema SCHEMA = LogicalSchema.of(SchemaBuilder
      .struct()
      .field("F1", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private SourceTopicsExtractor extractor;
  private KsqlEngine ksqlEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
    extractor = new SourceTopicsExtractor(metaStore);

    givenTopic("topic1", TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);

    givenTopic("topic2", TOPIC_2);
    givenStreamWithTopic(STREAM_TOPIC_2, TOPIC_2);
  }

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  private Statement givenStatement(final String sql) {
    return ksqlEngine.prepare(ksqlEngine.parse(sql).get(0)).getStatement();
  }

  @Test
  public void shouldExtractTopicFromSimpleSelect() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimaryKafkaTopicName(), is(TOPIC_1.name()));
  }

  @Test
  public void shouldExtractPrimaryTopicFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimaryKafkaTopicName(), is(TOPIC_1.name()));
  }

  @Test
  public void shouldExtractJoinTopicsFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getSourceTopics(), contains(TOPIC_1.name(), TOPIC_2.name()));
  }

  @Test
  public void shouldFailIfSourceTopicNotInMetastore() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");
    metaStore.deleteSource(STREAM_TOPIC_1.toUpperCase());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(STREAM_TOPIC_1.toUpperCase() + " does not exist.");

    // When:
    extractor.process(statement, null);
  }

  private void givenStreamWithTopic(
      final String streamName,
      final TopicDescription topicDescription
  ) {
    final KsqlTopic sourceTopic = new KsqlTopic(
        topicDescription.name(),
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KsqlStream<?> streamSource = new KsqlStream<>(
        "",
        streamName.toUpperCase(),
        SCHEMA,
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        sourceTopic
    );

    metaStore.putSource(streamSource);
  }

  private static void givenTopic(final String topicName, final TopicDescription topicDescription) {
    when(topicDescription.name()).thenReturn(topicName);
  }
}
