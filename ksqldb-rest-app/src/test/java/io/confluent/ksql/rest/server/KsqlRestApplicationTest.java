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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogServerUtils;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.PersistentQueryCleanupImpl;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestApplicationTest {
  private static final String LOG_STREAM_NAME = "log_stream";
  private static final String LOG_TOPIC_NAME = "log_topic";
  private static final String CMD_TOPIC_NAME = "command_topic";
  private static final String LIST_STREAMS_SQL = "list streams;";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private CommandRunner commandRunner;
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
  private KsqlSecurityExtension securityExtension;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ServerState serverState;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KsqlServerPrecondition precondition1;
  @Mock
  private KsqlServerPrecondition precondition2;
  @Mock
  private Consumer<KsqlConfig> rocksDBConfigSetterHandler;
  @Mock
  private HeartbeatAgent heartbeatAgent;
  @Mock
  private LagReportingAgent lagReportingAgent;
  @Mock
  private EndpointResponse response;
  @Mock
  private DenyListPropertyValidator denyListPropertyValidator;
  @Mock
  private QueryExecutor queryExecutor;
  @Mock
  private KafkaTopicClient internalTopicClient;
  @Mock
  private Admin internalAdminClient;

  @Mock
  private Vertx vertx;

  private String logCreateStatement;
  private KsqlRestApplication app;
  private KsqlRestConfig restConfig;
  private KsqlSecurityContext securityContext;

  private final ArgumentCaptor<KsqlSecurityContext> securityContextArgumentCaptor =
      ArgumentCaptor.forClass(KsqlSecurityContext.class);
  private final ArgumentCaptor<PersistentQueryCleanupImpl> queryCleanupArgumentCaptor =
    ArgumentCaptor.forClass(PersistentQueryCleanupImpl.class);

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    doAnswer(a -> {
      ((Handler<Void>) a.getArgument(0)).handle(null);
      return null;
    }).when(vertx).close(any());
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(true);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME))
        .thenReturn(LOG_TOPIC_NAME);

    when(processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)).thenReturn(true);
    when(processingLogContext.getConfig()).thenReturn(processingLogConfig);

    when(commandQueue.getCommandTopicName()).thenReturn(CMD_TOPIC_NAME);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);

    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("ksql-id");
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(ImmutableMap.of("state.dir", "/tmp/cat"));

    when(precondition1.checkPrecondition(any(), any(), any())).thenReturn(Optional.empty());
    when(precondition2.checkPrecondition(any(), any(), any())).thenReturn(Optional.empty());

    when(response.getStatus()).thenReturn(200);
    when(response.getEntity()).thenReturn(new KsqlEntityList(
        Collections.singletonList(new StreamsList(
            LIST_STREAMS_SQL,
            Collections.emptyList()))));

    when(ksqlResource.handleKsqlStatements(
        any(),
        any())
    ).thenReturn(response);

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    logCreateStatement = ProcessingLogServerUtils.processingLogStreamCreateStatement(
        processingLogConfig,
        ksqlConfig
    );

    givenAppWithRestConfig(ImmutableMap.of(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0"), new MetricCollectors());
  }

  @Test
  public void shouldCloseServiceContextOnClose() {
    // When:
    app.shutdown();

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldCloseSecurityExtensionOnClose() {
    // When:
    app.shutdown();

    // Then:
    verify(securityExtension).close();
  }

  @Test
  public void shouldAddConfigurableMetricsReportersIfPresentInKsqlConfig() {
    // When:
    final MetricsReporter mockReporter = mock(MetricsReporter.class);
    when(ksqlConfig.getConfiguredInstances(anyString(), any(), any()))
        .thenReturn(Collections.singletonList(mockReporter));
    final MetricCollectors metricCollectors = new MetricCollectors();
    givenAppWithRestConfig(Collections.emptyMap(), metricCollectors);

    // Then:
    final List<MetricsReporter> reporters = metricCollectors.getMetrics().reporters();
    assertThat(reporters, hasItem(mockReporter));
  }

  @Test
  public void shouldCreateLogStreamThroughKsqlResource() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null))
    );
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
  }

  @Test
  public void shouldNotCreateLogStreamIfAutoCreateNotConfigured() {
    // Given:
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(false);

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource, never()).handleKsqlStatements(
        securityContext,
        new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldNotCreateLogStreamIfAlreadyExists() {
    // Given:

    final StreamsList streamsList =
        new StreamsList(
            LIST_STREAMS_SQL,
            Collections.singletonList(new SourceInfo.Stream(LOG_STREAM_NAME, "", "", "", false)));
    when(response.getEntity()).thenReturn(new KsqlEntityList(Collections.singletonList(streamsList)));
    
    when(ksqlResource.handleKsqlStatements(
        any(),
        any())
    ).thenReturn(response);

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource, never()).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null))
    );
  }
  
  @Test
  public void shouldStartCommandStoreAndCommandRunnerBeforeCreatingLogStream() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandQueue, commandRunner, ksqlResource);
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandRunner).processPriorCommands(queryCleanupArgumentCaptor.capture());
    inOrder.verify(commandRunner).start();
    inOrder.verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null))
    );
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
  }

  @Test
  public void shouldCreateLogTopicBeforeSendingCreateStreamRequest() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(topicClient, ksqlResource);
    inOrder.verify(topicClient).createTopic(eq(LOG_TOPIC_NAME), anyInt(), anyShort());
    inOrder.verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null))
    );
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
  }

  @Test
  public void shouldInitializeCommandStoreCorrectly() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(internalTopicClient, commandQueue, commandRunner);
    inOrder.verify(internalTopicClient).createTopic(eq(CMD_TOPIC_NAME), anyInt(), anyShort(), anyMap());
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandRunner).processPriorCommands(queryCleanupArgumentCaptor.capture());
  }

  @Test
  public void shouldReplayCommandsBeforeSettingReady() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandRunner, serverState);
    inOrder.verify(commandRunner).processPriorCommands(queryCleanupArgumentCaptor.capture());
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldSendCreateStreamRequestBeforeSettingReady() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(ksqlResource, serverState);
    verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null))
    );
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldCheckPreconditionsBeforeUsingServiceContext() {
    // Given:
    when(precondition2.checkPrecondition(any(), any(), any())).then(a -> {
      verifyNoMoreInteractions(serviceContext);
      return Optional.empty();
    });

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serviceContext);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext, internalTopicClient);
  }

  @Test
  public void shouldNotInitializeUntilPreconditionsChecked() {
    // Given:
    final KsqlErrorMessage error1 = new KsqlErrorMessage(50000, "error1");
    final KsqlErrorMessage error2 = new KsqlErrorMessage(50000, "error2");
    final Queue<KsqlErrorMessage> errors = new LinkedList<>();
    errors.add(error1);
    errors.add(error2);
    when(precondition2.checkPrecondition(any(), any(), any())).then(a -> {
      verifyNoMoreInteractions(serviceContext);
      return Optional.ofNullable(errors.isEmpty() ? null : errors.remove());
    });

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serverState);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(serverState).setInitializingReason(error1);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(serverState).setInitializingReason(error2);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext, internalTopicClient);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext, internalTopicClient);
  }

  @Test
  public void shouldConfigureRocksDBConfigSetter() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(rocksDBConfigSetterHandler).accept(ksqlConfig);
  }

  @Test
  public void shouldConfigureIQWithInterNodeListenerIfSet() {
    // Given:
    givenAppWithRestConfig(
        ImmutableMap.of(
            KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0",
            KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "https://some.host:12345"
        ),
        new MetricCollectors()
    );

    // When:
    final KsqlConfig ksqlConfig = app.buildConfigWithPort();

    // Then:
    assertThat(
        ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG),
        is("https://some.host:12345")
    );
  }

  @Test
  public void shouldConfigureIQWithFirstListenerIfInterNodeNotSet() {
    // Given:
    givenAppWithRestConfig(
        ImmutableMap.of(
            KsqlRestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"
        ),
        new MetricCollectors()
    );

    // When:
    final KsqlConfig ksqlConfig = app.buildConfigWithPort();

    // Then:
    assertThat(
        ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG),
        is("http://some.host:1244")
    );
  }

  private void givenAppWithRestConfig(
      final Map<String, Object> restConfigMap,
      final MetricCollectors metricCollectors) {

    restConfig = new KsqlRestConfig(restConfigMap);

    app = new KsqlRestApplication(
        serviceContext,
        ksqlEngine,
        ksqlConfig,
        restConfig,
        commandRunner,
        commandQueue,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        versionCheckerAgent,
        apiSecurityContext -> new KsqlSecurityContext(Optional.empty(), serviceContext),
        securityExtension,
        Optional.empty(),
        serverState,
        processingLogContext,
        ImmutableList.of(precondition1, precondition2),
        ImmutableList.of(ksqlResource, streamedQueryResource),
        rocksDBConfigSetterHandler,
        Optional.of(heartbeatAgent),
        Optional.of(lagReportingAgent),
        vertx,
        denyListPropertyValidator,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        queryExecutor,
        metricCollectors,
        internalTopicClient,
        internalAdminClient
    );
  }

}
