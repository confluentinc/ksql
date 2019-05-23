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

package io.confluent.ksql.rest.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.rest.RestConfig;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestApplicationTest {
  private static final String LOG_STREAM_NAME = "log_stream";
  private static final String LOG_TOPIC_NAME = "log_topic";
  private static final String CMD_TOPIC_NAME = "command_topic";

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
  private CommandStore commandQueue;
  @Mock
  private QueuedCommandStatus queuedCommandStatus;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ServerState serverState;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private DdlCommandExec ddlCommandExec;
  @Mock
  private KsqlServerPrecondition precondition1;
  @Mock
  private KsqlServerPrecondition precondition2;
  private PreparedStatement logCreateStatement;
  private KsqlRestApplication app;

  @Before
  public void setUp() {
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(true);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME))
        .thenReturn(LOG_TOPIC_NAME);
    when(processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)).thenReturn(true);
    when(processingLogContext.getConfig()).thenReturn(processingLogConfig);
    when(ksqlEngine.createSandbox()).thenReturn(sandBox);
    when(commandQueue.isEmpty()).thenReturn(true);
    when(commandQueue.enqueueCommand(any(), any(), any()))
        .thenReturn(queuedCommandStatus);
    when(commandQueue.getCommandTopicName()).thenReturn(CMD_TOPIC_NAME);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(CMD_TOPIC_NAME)).thenReturn(false);
    when(ksqlEngine.getDdlCommandExec()).thenReturn(ddlCommandExec);
    when(precondition1.checkPrecondition(any(), any())).thenReturn(Optional.empty());
    when(precondition2.checkPrecondition(any(), any())).thenReturn(Optional.empty());

    logCreateStatement = ProcessingLogServerUtils.processingLogStreamCreateStatement(
        processingLogConfig,
        ksqlConfig
    );

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
        versionCheckerAgent,
        serverState,
        processingLogContext,
        ImmutableList.of(precondition1, precondition2)
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
  public void shouldCreateLogStream() {
    // When:
    app.startKsql();

    // Then:
    verify(commandQueue).isEmpty();
    verify(sandBox).execute(logCreateStatement, ksqlConfig, Collections.emptyMap());
    verify(commandQueue).enqueueCommand(logCreateStatement, ksqlConfig, Collections.emptyMap());
  }

  @Test
  public void shouldNotCreateLogStreamIfAutoCreateNotConfigured() {
    // Given:
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(false);

    // When:
    app.startKsql();

    // Then:
    verify(commandQueue, never()).enqueueCommand(any(), any(), any());
  }

  @Test
  public void shouldOnlyCreateLogStreamIfCommandTopicEmpty() {
    // Given:
    when(commandQueue.isEmpty()).thenReturn(false);

    // When:
    app.startKsql();

    // Then:
    verify(commandQueue, never()).enqueueCommand(any(), any(), any());
  }

  @Test
  public void shouldNotCreateLogStreamIfValidationFails() {
    // Given:
    when(sandBox.execute(any(), any(), any())).thenThrow(new KsqlException("error"));

    // When:
    app.startKsql();

    // Then:
    verify(commandQueue, never()).enqueueCommand(any(), any(), any());
  }

  @Test
  public void shouldStartCommandStoreBeforeEnqueuingLogStream() {
    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandQueue);
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandQueue).enqueueCommand(
        logCreateStatement,
        ksqlConfig,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldCreateLogTopicBeforeEnqueuingLogStream() {
    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(topicClient, commandQueue);
    inOrder.verify(topicClient).createTopic(eq(LOG_TOPIC_NAME), anyInt(), anyShort());
    inOrder.verify(commandQueue).enqueueCommand(
        logCreateStatement,
        ksqlConfig,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldInitializeCommandStoreCorrectly() {
    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(topicClient, commandQueue, commandRunner);
    inOrder.verify(topicClient).createTopic(eq(CMD_TOPIC_NAME), anyInt(), anyShort(), anyMap());
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandRunner).processPriorCommands();
  }

  @Test
  public void shouldReplayCommandsBeforeSettingReady() {
    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandRunner, serverState);
    inOrder.verify(commandRunner).processPriorCommands();
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldEnqueueLogStreamBeforeSettingReady() {
    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandQueue, serverState);
    inOrder.verify(commandQueue).enqueueCommand(
        logCreateStatement,
        ksqlConfig,
        Collections.emptyMap()
    );
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldCheckPreconditionsBeforeUsingServiceContext() {
    // Given:
    when(precondition2.checkPrecondition(any(), any())).then(a -> {
      verifyZeroInteractions(serviceContext);
      return Optional.empty();
    });

    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serviceContext);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
  }

  @Test
  public void shouldNotInitializeUntilPreconditionsChecked() {
    // Given:
    final Queue<String> errors = new LinkedList<>();
    errors.add("error1");
    errors.add("error2");
    when(precondition2.checkPrecondition(any(), any())).then(a -> {
      verifyZeroInteractions(serviceContext);
      return Optional.ofNullable(errors.isEmpty() ? null : errors.remove());
    });

    // When:
    app.startKsql();

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serverState);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(serverState).setInitializingReason("error1");
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(serverState).setInitializingReason("error2");
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
  }
}
