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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogServerUtils;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.vertx.core.Vertx;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestApplicationTest {
  private static final String LOG_STREAM_NAME = "log_stream";
  private static final String LOG_TOPIC_NAME = "log_topic";
  private static final String CMD_TOPIC_NAME = "command_topic";
  private static final String LIST_STREAMS_SQL = "list streams;";
  private final ArgumentCaptor<KsqlSecurityContext> securityContextArgumentCaptor =
      ArgumentCaptor.forClass(KsqlSecurityContext.class);
  @Mock private ServiceContext serviceContext;
  @Mock private KsqlEngine ksqlEngine;
  @Mock private KsqlConfig ksqlConfig;
  @Mock private ProcessingLogConfig processingLogConfig;
  @Mock private CommandRunner commandRunner;
  @Mock private StatusResource statusResource;
  @Mock private StreamedQueryResource streamedQueryResource;
  @Mock private KsqlResource ksqlResource;
  @Mock private VersionCheckerAgent versionCheckerAgent;
  @Mock private CommandStore commandQueue;
  @Mock private KsqlSecurityExtension securityExtension;
  @Mock private ProcessingLogContext processingLogContext;
  @Mock private ServerState serverState;
  @Mock private KafkaTopicClient topicClient;
  @Mock private KsqlServerPrecondition precondition1;
  @Mock private KsqlServerPrecondition precondition2;
  @Mock private ParsedStatement parsedStatement;
  @Mock private PreparedStatement<?> preparedStatement;
  @Mock private Consumer<KsqlConfig> rocksDBConfigSetterHandler;
  @Mock private HeartbeatAgent heartbeatAgent;
  @Mock private LagReportingAgent lagReportingAgent;
  @Mock private EndpointResponse response;
  @Mock private DenyListPropertyValidator denyListPropertyValidator;
  @Mock private RoutingFilterFactory routingFilterFactory;
  @Mock private RateLimiter rateLimiter;
  @Mock private ConcurrencyLimiter concurrencyLimiter;
  @Mock private HARouting haRouting;
  @Mock private PushRouting pushRouting;
  @Mock private Vertx vertx;
  private String logCreateStatement;
  private KsqlRestApplication app;
  private KsqlRestConfig restConfig;
  private KsqlSecurityContext securityContext;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void setUp() {
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE)).thenReturn(true);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME)).thenReturn(LOG_TOPIC_NAME);

    when(processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)).thenReturn(true);
    when(processingLogContext.getConfig()).thenReturn(processingLogConfig);

    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(parsedStatement));
    when(ksqlEngine.prepare(any())).thenReturn((PreparedStatement) preparedStatement);

    when(commandQueue.getCommandTopicName()).thenReturn(CMD_TOPIC_NAME);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(CMD_TOPIC_NAME)).thenReturn(false);

    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("ksql-id");
    when(ksqlConfig.getKsqlStreamConfigProps())
        .thenReturn(ImmutableMap.of(StreamsConfig.STATE_DIR_CONFIG, "/tmp/cat/"));

    when(precondition1.checkPrecondition(any(), any())).thenReturn(Optional.empty());
    when(precondition2.checkPrecondition(any(), any())).thenReturn(Optional.empty());

    when(response.getStatus()).thenReturn(200);
    when(response.getEntity())
        .thenReturn(
            new KsqlEntityList(
                Collections.singletonList(
                    new StreamsList(LIST_STREAMS_SQL, Collections.emptyList()))));

    when(ksqlResource.handleKsqlStatements(any(), any())).thenReturn(response);

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    logCreateStatement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            processingLogConfig, ksqlConfig);
    MetricCollectors.initialize();

    givenAppWithRestConfig(ImmutableMap.of(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0"));
  }

  @After
  public void tearDown() {
    MetricCollectors.cleanUp();
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
    givenAppWithRestConfig(Collections.emptyMap());

    // Then:
    final List<MetricsReporter> reporters = MetricCollectors.getMetrics().reporters();
    assertThat(reporters, hasItem(mockReporter));
  }

  @Test
  public void shouldCreateLogStreamThroughKsqlResource() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource)
        .handleKsqlStatements(
            securityContextArgumentCaptor.capture(),
            eq(
                new KsqlRequest(
                    logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)));
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
  }

  @Test
  public void shouldNotCreateLogStreamIfAutoCreateNotConfigured() {
    // Given:
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE)).thenReturn(false);

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource, never())
        .handleKsqlStatements(
            securityContext,
            new KsqlRequest(
                logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null));
  }

  @Test
  public void shouldNotCreateLogStreamIfAlreadyExists() {
    // Given:

    final StreamsList streamsList =
        new StreamsList(
            LIST_STREAMS_SQL,
            Collections.singletonList(new SourceInfo.Stream(LOG_STREAM_NAME, "", "", "", false)));
    when(response.getEntity())
        .thenReturn(new KsqlEntityList(Collections.singletonList(streamsList)));

    when(ksqlResource.handleKsqlStatements(any(), any())).thenReturn(response);

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource, never())
        .handleKsqlStatements(
            securityContextArgumentCaptor.capture(),
            eq(
                new KsqlRequest(
                    logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)));
  }

  @Test
  public void shouldStartCommandStoreAndCommandRunnerBeforeCreatingLogStream() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandQueue, commandRunner, ksqlResource);
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandRunner).processPriorCommands();
    inOrder.verify(commandRunner).start();
    inOrder
        .verify(ksqlResource)
        .handleKsqlStatements(
            securityContextArgumentCaptor.capture(),
            eq(
                new KsqlRequest(
                    logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)));
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
    inOrder
        .verify(ksqlResource)
        .handleKsqlStatements(
            securityContextArgumentCaptor.capture(),
            eq(
                new KsqlRequest(
                    logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)));
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
  }

  @Test
  public void shouldInitializeCommandStoreCorrectly() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(topicClient, commandQueue, commandRunner);
    inOrder.verify(topicClient).createTopic(eq(CMD_TOPIC_NAME), anyInt(), anyShort(), anyMap());
    inOrder.verify(commandQueue).start();
    inOrder.verify(commandRunner).processPriorCommands();
  }

  @Test
  public void shouldReplayCommandsBeforeSettingReady() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(commandRunner, serverState);
    inOrder.verify(commandRunner).processPriorCommands();
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldSendCreateStreamRequestBeforeSettingReady() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(ksqlResource, serverState);
    verify(ksqlResource)
        .handleKsqlStatements(
            securityContextArgumentCaptor.capture(),
            eq(
                new KsqlRequest(
                    logCreateStatement, Collections.emptyMap(), Collections.emptyMap(), null)));
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
    inOrder.verify(serverState).setReady();
  }

  @Test
  public void shouldCheckPreconditionsBeforeUsingServiceContext() {
    // Given:
    when(precondition2.checkPrecondition(any(), any()))
        .then(
            a -> {
              verifyNoMoreInteractions(serviceContext);
              return Optional.empty();
            });

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serviceContext);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
  }

  @Test
  public void shouldNotInitializeUntilPreconditionsChecked() {
    // Given:
    final KsqlErrorMessage error1 = new KsqlErrorMessage(50000, "error1");
    final KsqlErrorMessage error2 = new KsqlErrorMessage(50000, "error2");
    final Queue<KsqlErrorMessage> errors = new LinkedList<>();
    errors.add(error1);
    errors.add(error2);
    when(precondition2.checkPrecondition(any(), any()))
        .then(
            a -> {
              verifyNoMoreInteractions(serviceContext);
              return Optional.ofNullable(errors.isEmpty() ? null : errors.remove());
            });

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final InOrder inOrder = Mockito.inOrder(precondition1, precondition2, serverState);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(serverState).setInitializingReason(error1);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(serverState).setInitializingReason(error2);
    inOrder.verify(precondition1).checkPrecondition(restConfig, serviceContext);
    inOrder.verify(precondition2).checkPrecondition(restConfig, serviceContext);
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
            KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "https://some.host:12345"));

    // When:
    final KsqlConfig ksqlConfig = app.buildConfigWithPort();

    // Then:
    assertThat(
        ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG),
        is("https://some.host:12345"));
  }

  @Test
  public void shouldConfigureIQWithFirstListenerIfInterNodeNotSet() {
    // Given:
    givenAppWithRestConfig(
        ImmutableMap.of(
            KsqlRestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"));

    // When:
    final KsqlConfig ksqlConfig = app.buildConfigWithPort();

    // Then:
    assertThat(
        ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG),
        is("http://some.host:1244"));
  }

  @Test
  public void shouldDeleteExtraStateStores() {
    // Given:
    givenAppWithRestConfig(
            ImmutableMap.of(
                    KsqlRestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"));

    File tempFile = new File(ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.STATE_DIR_CONFIG).toString());
    if (!tempFile.exists()){
      tempFile.mkdirs();
    }
    File fakeStateStore = new File(tempFile.getAbsolutePath() + "/fakeStateStore");
    if (!fakeStateStore.exists()){
      fakeStateStore.mkdirs();
    }
    assertTrue(fakeStateStore.exists());

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    assertFalse(fakeStateStore.exists());
  }

  @Test
  public void shouldKeepStateStoresBelongingToRunningQueries() {
    // Given:
    givenAppWithRestConfig(
            ImmutableMap.of(
                    KsqlRestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"));
    final PersistentQueryMetadata runningQuery = mock(PersistentQueryMetadata.class);
    when(runningQuery.getQueryId()).thenReturn(new QueryId("testQueryID"));
    List<PersistentQueryMetadata> queryMocks = ImmutableList.of(runningQuery);
    when(ksqlEngine.getPersistentQueries()).thenReturn(queryMocks);
    File tempFile = new File(ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.STATE_DIR_CONFIG).toString());
    if (!tempFile.exists()){
      tempFile.mkdirs();
    }
    File fakeStateStore = new File(tempFile.getAbsolutePath() + runningQuery.getQueryId().toString());
    if (!fakeStateStore.exists()){
      fakeStateStore.mkdirs();
    }
    // When:
    app.startKsql(ksqlConfig);
    // Then:
    assertTrue(fakeStateStore.exists());
  }

  @Test
  public void shouldThrowExceptionIfNoStateStoreDir() {
    // Given:
    final TestAppender appender = new TestAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    givenAppWithRestConfig(
            ImmutableMap.of(
                    KsqlRestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"));

    // When:
    app.startKsql(ksqlConfig);

    // Then:
    final List<LoggingEvent> log = appender.getLog();
    // will probably be 2 instead of 0
    final LoggingEvent firstLogEntry = log.get(0);
    assertThat(firstLogEntry.getLevel(), is(Level.ERROR));
    assertThat((String) firstLogEntry.getMessage(), is("Failed to clean a state directory /tmp/cat"));
  }

  private void givenAppWithRestConfig(final Map<String, Object> restConfigMap) {

    restConfig = new KsqlRestConfig(restConfigMap);

    app =
        new KsqlRestApplication(
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
            routingFilterFactory,
            rateLimiter,
            concurrencyLimiter,
            haRouting,
            pushRouting,
            Optional.empty());
  }

  class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
      log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
      return new ArrayList<LoggingEvent>(log);
    }
  }
}
