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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.context.KsqlSecurityContextBinder;
import io.confluent.ksql.rest.server.filters.KsqlAuthorizationFilter;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.rest.RestConfig;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.ws.rs.core.Configurable;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
  private ParsedStatement parsedStatement;
  @Mock
  private PreparedStatement<?> preparedStatement;
  @Mock
  private Consumer<KsqlConfig> rocksDBConfigSetterHandler;
  @Mock
  private HeartbeatAgent heartbeatAgent;
  @Mock
  private LagReportingAgent lagReportingAgent;

  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  private Supplier<SchemaRegistryClient> schemaRegistryClientFactory;
  private String logCreateStatement;
  private KsqlRestApplication app;
  private KsqlRestConfig restConfig;
  private KsqlSecurityContext securityContext;

  private final ArgumentCaptor<KsqlSecurityContext> securityContextArgumentCaptor =
      ArgumentCaptor.forClass(KsqlSecurityContext.class);

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    schemaRegistryClientFactory = () -> schemaRegistryClient;
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(true);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME))
        .thenReturn(LOG_TOPIC_NAME);

    when(processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)).thenReturn(true);
    when(processingLogContext.getConfig()).thenReturn(processingLogConfig);

    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(parsedStatement));
    when(ksqlEngine.prepare(any())).thenReturn((PreparedStatement) preparedStatement);

    when(commandQueue.getCommandTopicName()).thenReturn(CMD_TOPIC_NAME);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(CMD_TOPIC_NAME)).thenReturn(false);
    when(precondition1.checkPrecondition(any(), any())).thenReturn(Optional.empty());
    when(precondition2.checkPrecondition(any(), any())).thenReturn(Optional.empty());

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    logCreateStatement = ProcessingLogServerUtils.processingLogStreamCreateStatement(
        processingLogConfig,
        ksqlConfig
    );

    givenAppWithRestConfig(ImmutableMap.of(RestConfig.LISTENERS_CONFIG,  "http://localhost:0"));
  }

  @Test
  public void shouldCloseServiceContextOnClose() {
    // When:
    app.triggerShutdown();

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldCloseSecurityExtensionOnClose() {
    // When:
    app.triggerShutdown();

    // Then:
    verify(securityExtension).close();
  }

  @Test
  public void shouldNotRegisterAuthorizationFilterWithoutAuthorizationProvider() {
    // Given:
    final Configurable<?> configurable = mock(Configurable.class);

    // When:
    app.configureBaseApplication(configurable, Collections.emptyMap());

    // Then:
    verify(configurable, times(0)).register(any(KsqlAuthorizationFilter.class));
  }

  @Test
  public void shouldRegisterAuthorizationFilterWithAuthorizationProvider() {
    // Given:
    final Configurable<?> configurable = mock(Configurable.class);
    final KsqlAuthorizationProvider provider = mock(KsqlAuthorizationProvider.class);
    when(securityExtension.getAuthorizationProvider()).thenReturn(Optional.of(provider));

    // When:
    app.configureBaseApplication(configurable, Collections.emptyMap());

    // Then:
    verify(configurable).register(any(KsqlAuthorizationFilter.class));
  }

  @Test
  public void shouldCreateLogStreamThroughKsqlResource() {
    // When:
    app.startKsql(ksqlConfig);

    // Then:
    verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), null))
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
        new KsqlRequest(logCreateStatement, Collections.emptyMap(), null)
    );
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
    inOrder.verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), null))
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
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), null))
    );
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
    verify(ksqlResource).handleKsqlStatements(
        securityContextArgumentCaptor.capture(),
        eq(new KsqlRequest(logCreateStatement, Collections.emptyMap(), null))
    );
    assertThat(securityContextArgumentCaptor.getValue().getUserPrincipal(), is(Optional.empty()));
    assertThat(securityContextArgumentCaptor.getValue().getServiceContext(), is(serviceContext));
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
    when(precondition2.checkPrecondition(any(), any())).then(a -> {
      verifyZeroInteractions(serviceContext);
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
    givenAppWithRestConfig(ImmutableMap.of(
        RestConfig.LISTENERS_CONFIG, "http://localhost:0",
        KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "https://some.host:12345"
    ));

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
    givenAppWithRestConfig(ImmutableMap.of(
        RestConfig.LISTENERS_CONFIG, "http://some.host:1244,https://some.other.host:1258"
    ));

    // When:
    final KsqlConfig ksqlConfig = app.buildConfigWithPort();

    // Then:
    assertThat(
        ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG),
        is("http://some.host:1244")
    );
  }

  private void givenAppWithRestConfig(final Map<String, Object> restConfigMap) {

    restConfig = new KsqlRestConfig(restConfigMap);

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
        (config, securityExtension) ->
            new KsqlSecurityContextBinder(config, securityExtension, schemaRegistryClientFactory),
        securityExtension,
        serverState,
        processingLogContext,
        ImmutableList.of(precondition1, precondition2),
        ImmutableList.of(ksqlResource, streamedQueryResource),
        rocksDBConfigSetterHandler,
        Optional.of(heartbeatAgent),
        Optional.of(lagReportingAgent)
    );
  }
}
