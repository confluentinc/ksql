/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static org.easymock.EasyMock.anyObject;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.util.ProcessingLogConfig;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.rest.RestConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestApplicationTest {
  private static final String COMMAND_TOPIC = "command_topic";
  private static final String LOG_STREAM_NAME = "log_stream";
  private static final String LOG_TOPIC_NAME = "log_topic";

  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);
  private final Map<String, ?> commandTopicConfig = Collections.singletonMap(
      TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE);
  private final KsqlRestConfig restConfig =
      new KsqlRestConfig(
          Collections.singletonMap(RestConfig.LISTENERS_CONFIG,
          "http://localhost:8088"));

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlExecutionContext sandBox;
  @Mock
  private Statement statement;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private CommandRunner commandRunner;
  @Mock
  private RootDocument rootDocument;
  @Mock
  private StatusResource statusResource;
  @Mock
  private StreamedQueryResource streamedQueryResource;
  @Mock
  private KsqlResource ksqlResource;
  @Mock
  private VersionCheckerAgent versionCheckerAgent;
  @Mock
  private CommandQueue commandQueue;
  @Mock
  private QueuedCommandStatus queuedCommandStatus;
  private KsqlRestApplication app;

  @Before
  public void setUp() {
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("default_id_");
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(ProcessingLogConfig.AUTO_CREATE_ON);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME))
        .thenReturn(LOG_TOPIC_NAME);
    when(ksqlEngine.parseStatements(any()))
        .then(
            invocation -> ImmutableList.of(
                new PreparedStatement<>(invocation.getArgument(0), statement)
            )
        );
    when(ksqlEngine.createSandbox()).thenReturn(sandBox);
    when(commandQueue.isEmpty()).thenReturn(true);
    when(commandQueue.enqueueCommand(any(), any(), any(), any()))
        .thenReturn(queuedCommandStatus);
    when(serviceContext.getAdminClient())
        .thenReturn(new FakeKafkaClientSupplier().getAdminClient(Collections.emptyMap()));

    app = new KsqlRestApplication(
        serviceContext,
        ksqlEngine,
        ksqlConfig,
        restConfig,
        commandRunner,
        commandQueue,
        rootDocument,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        versionCheckerAgent
    );
  }

  @Test
  public void shouldCloseServiceContextOnClose() {
    // When:
    app.stop();

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldCreateCommandTopicIfItDoesNotExist() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 1,
        commandTopicConfig);
    EasyMock.expectLastCall();
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldNotAttemptToCreateCommandTopicIfItExists() {
    EasyMock.resetToStrict(topicClient);
    EasyMock.expect(topicClient.isTopicExists(COMMAND_TOPIC)).andReturn(true);
    EasyMock.expect(topicClient.addTopicConfig(anyObject(), anyObject())).andReturn(false);

    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldEnsureCommandTopicHasInfiniteRetention() {
    final Map<String, Object> retentionConfig = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE
    );
    EasyMock.expect(topicClient.isTopicExists(COMMAND_TOPIC)).andReturn(true);
    EasyMock.expect(topicClient.addTopicConfig(COMMAND_TOPIC, retentionConfig)).andReturn(true);

    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateCommandTopicWithNumReplicasFromConfig() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 3,
        commandTopicConfig);
    EasyMock.expectLastCall();
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(
        new KsqlRestConfig(
            new HashMap(){{
              put(RestConfig.LISTENERS_CONFIG, "http://localhost:8080");
              put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, 3);
            }}),
        topicClient,
        COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldNotFailIfTopicExistsOnCreation() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 1,
        commandTopicConfig);

    EasyMock.expectLastCall().andThrow(new KafkaTopicExistsException("blah"));
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldCreateLogStream() {
    // When:
    KsqlRestApplication.maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandQueue
    );

    // Then:
    verify(commandQueue).isEmpty();
    final String statementText = ProcessingLogServerUtils.processingLogStreamCreateStatement(
        LOG_STREAM_NAME,
        LOG_TOPIC_NAME
    );
    verify(ksqlEngine).parseStatements(statementText);
    verify(sandBox).execute(new PreparedStatement<>(statementText, statement),
        ksqlConfig,
        Collections.emptyMap());
    verify(commandQueue).enqueueCommand(
        statementText,
        statement,
        ksqlConfig,
        Collections.emptyMap());
  }

  @Test
  public void shouldNotCreateLogStreamIfAutoCreateNotConfigured() {
    // Given:
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(ProcessingLogConfig.AUTO_CREATE_OFF);

    // When:
    KsqlRestApplication.maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandQueue
    );

    // Then:
    verifyNoMoreInteractions(ksqlEngine, commandQueue);
  }

  @Test
  public void shouldOnlyCreateLogStreamIfCommandTopicEmpty() {
    // Given:
    when(commandQueue.isEmpty()).thenReturn(false);

    // When:
    KsqlRestApplication.maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandQueue
    );

    // Then:
    verify(commandQueue, never()).enqueueCommand(any(), any(), any(), any());
  }

  @Test
  public void shouldNotCreateLogStreamIfValidationFails() {
    // Given:
    when(sandBox.execute(any(), any(), any())).thenThrow(new KsqlException("error"));

    // When:
    KsqlRestApplication.maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandQueue
    );

    // Then:
    verify(commandQueue, never()).enqueueCommand(any(), any(), any(), any());
  }
}
