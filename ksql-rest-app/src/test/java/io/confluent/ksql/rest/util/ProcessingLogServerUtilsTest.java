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

package io.confluent.ksql.rest.util;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingLogServerUtilsTest {

  private static final String STREAM = "PROCESSING_LOG_STREAM";
  private static final String TOPIC = "processing_log_topic";
  private static final String CLUSTER_ID = "ksql_cluster.";
  private static final int PARTITIONS = 10;
  private static final short REPLICAS = 3;
  private static final String DEFAULT_TOPIC =
      CLUSTER_ID + ProcessingLogConfig.TOPIC_NAME_DEFAULT_SUFFIX;

  private final ServiceContext serviceContext = TestServiceContext.create();
  private final KafkaTopicClient spyTopicClient = spy(serviceContext.getTopicClient());
  private final MutableMetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
  private final KsqlEngine ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
      serviceContext,
      metaStore
  );
  private final ProcessingLogConfig config = new ProcessingLogConfig(
      ImmutableMap.of(
          ProcessingLogConfig.TOPIC_AUTO_CREATE,
          true,
          ProcessingLogConfig.TOPIC_NAME,
          TOPIC,
          ProcessingLogConfig.TOPIC_PARTITIONS,
          PARTITIONS,
          ProcessingLogConfig.TOPIC_REPLICATION_FACTOR,
          REPLICAS,
          ProcessingLogConfig.STREAM_NAME,
          STREAM
      )
  );
  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.KSQL_SERVICE_ID_CONFIG, CLUSTER_ID)
  );

  @Mock
  private KafkaTopicClient mockTopicClient;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @After
  public void teardown() {
    ksqlEngine.close();
  }

  private static List<String> push(final List<String> path, final String elem) {
    return new ImmutableList.Builder<String>().addAll(path).add(elem).build();
  }

  private void assertLogSchema(
      final Schema expected,
      final Schema schema,
      final List<String> path) {
    final String pathStr = String.join("->", path);
    assertThat("Type mismatch at " + pathStr, schema.type(), equalTo(expected.type()));
    switch (schema.type()) {
      case STRUCT:
        assertThat(
            "Struct field mismatch at " + String.join("->", path),
            schema.fields().stream()
                .map(f -> f.name().toUpperCase())
                .collect(toList()),
            equalTo(
                expected.fields().stream()
                    .map(f -> f.name().toUpperCase())
                    .collect(toList()))
        );
        for (int i = 0; i < schema.fields().size(); i++) {
          assertLogSchema(
              schema.fields().get(i).schema(),
              expected.fields().get(i).schema(),
              push(path, schema.fields().get(i).name())
          );
        }
        break;
      case MAP:
        assertLogSchema(expected.keySchema(), schema.keySchema(), push(path, "KEY"));
        assertLogSchema(expected.valueSchema(), schema.valueSchema(), push(path, "VALUE"));
        break;
      case ARRAY:
        assertLogSchema(expected.valueSchema(), schema.valueSchema(), push(path, "ELEMENTS"));
        break;
      default:
        break;
    }
  }

  private void assertLogStream(final String topicName) {
    final DataSource<?> dataSource = metaStore.getSource(STREAM);
    assertThat(dataSource, instanceOf(KsqlStream.class));
    final KsqlStream<?> stream = (KsqlStream) dataSource;
    final Schema expected = ProcessingLogServerUtils.getMessageSchema();
    assertThat(stream.getKsqlTopic().getValueFormat().getFormat(), is(Format.JSON));
    assertThat(stream.getKsqlTopic().getKafkaTopicName(), equalTo(topicName));
    assertThat(
        stream.getSchema().valueFields().stream().map(Field::name).collect(toList()),
        equalTo(
            new ImmutableList.Builder<String>()
                .addAll(
                    expected.fields().stream()
                        .map(f -> f.name().toUpperCase())
                        .collect(toList()))
                .build()
        )
    );
    expected.fields().forEach(
        f -> assertLogSchema(
            f.schema(),
            stream.getSchema().valueSchema().field(f.name().toUpperCase()).schema(),
            ImmutableList.of(f.name())));
  }

  @Test
  public void shouldBuildCorrectStreamCreateDDL() {
    // Given:
    serviceContext.getTopicClient().createTopic(TOPIC, 1, (short) 1);

    // When:
    final PreparedStatement<?> statement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            config,
            ksqlConfig);

    ksqlEngine.execute(ConfiguredStatement.of(statement, ImmutableMap.of(), ksqlConfig));

    // Then:
    assertThat(statement.getStatementText(), equalTo(
        "CREATE STREAM PROCESSING_LOG_STREAM ("
            + "logger VARCHAR, "
            + "level VARCHAR, "
            + "time BIGINT, "
            + "message STRUCT<"
            + "type INT, "
            + "deserializationError STRUCT<errorMessage VARCHAR, recordB64 VARCHAR, cause ARRAY<VARCHAR>>, "
            + "recordProcessingError STRUCT<errorMessage VARCHAR, record VARCHAR, cause ARRAY<VARCHAR>>, "
            + "productionError STRUCT<errorMessage VARCHAR>"
            + ">"
            + ") WITH(KAFKA_TOPIC='processing_log_topic', VALUE_FORMAT='JSON');"));

    assertLogStream(TOPIC);
  }

  @Test
  public void shouldBuildCorrectStreamCreateDDLWithDefaultTopicName() {
    // Given:
    serviceContext.getTopicClient().createTopic(DEFAULT_TOPIC, 1, (short)1);

    // When:
    final PreparedStatement<?> statement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            new ProcessingLogConfig(
                ImmutableMap.of(
                    ProcessingLogConfig.STREAM_AUTO_CREATE, true,
                    ProcessingLogConfig.STREAM_NAME, STREAM
                )
            ),
            ksqlConfig);

    ksqlEngine.execute(ConfiguredStatement.of(statement, ImmutableMap.of(), ksqlConfig));

    // Then:
    assertThat(statement.getStatementText(),
        containsString("KAFKA_TOPIC='ksql_cluster.ksql_processing_log'"));

    assertLogStream(DEFAULT_TOPIC);
  }

  @Test
  public void shouldNotCreateLogTopicIfNotConfigured() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        ImmutableMap.of(ProcessingLogConfig.TOPIC_AUTO_CREATE, false)
    );

    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        spyTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(false));
    verifyZeroInteractions(spyTopicClient);
  }

  @Test
  public void shouldThrowOnUnexpectedKafkaClientError() {
    doThrow(new RuntimeException("bad"))
        .when(mockTopicClient)
        .createTopic(anyString(), anyInt(), anyShort());
    expectedException.expectMessage("bad");
    expectedException.expect(RuntimeException.class);

    ProcessingLogServerUtils.maybeCreateProcessingLogTopic(mockTopicClient, config, ksqlConfig);
  }

  @Test
  public void shouldCreateProcessingLogTopic() {
    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        mockTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(true));
    assertThat(createdTopic.get(), equalTo(TOPIC));
    verify(mockTopicClient).createTopic(TOPIC, PARTITIONS, REPLICAS);
  }

  @Test
  public void shouldCreateProcessingLogTopicWithCorrectDefaultName() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        ImmutableMap.of(
            ProcessingLogConfig.TOPIC_AUTO_CREATE,
            true,
            ProcessingLogConfig.TOPIC_PARTITIONS,
            PARTITIONS,
            ProcessingLogConfig.TOPIC_REPLICATION_FACTOR,
            REPLICAS
        )
    );

    // When:
    final Optional<String> createdTopic = ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        mockTopicClient,
        config,
        ksqlConfig);

    // Then:
    assertThat(createdTopic.isPresent(), is(true));
    assertThat(createdTopic.get(), equalTo(DEFAULT_TOPIC));
    verify(mockTopicClient).createTopic(DEFAULT_TOPIC, PARTITIONS, REPLICAS);
  }
}
