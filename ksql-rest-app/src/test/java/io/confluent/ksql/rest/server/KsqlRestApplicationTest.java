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

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.context.KsqlRestServiceContextBinder;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.rest.RestConfig;
import java.util.Collections;
import javax.ws.rs.core.Configurable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestApplicationTest {
  private static final String LOG_STREAM_NAME = "log_stream";
  private static final String LOG_TOPIC_NAME = "log_topic";

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
  private CommandQueue commandQueue;
  @Mock
  private QueuedCommandStatus queuedCommandStatus;
  @Mock
  private KsqlSecurityExtension securityExtension;
  private KsqlRestApplication app;

  @Before
  public void setUp() {
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("default_id_");
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(true);
    when(processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME))
        .thenReturn(LOG_STREAM_NAME);
    when(processingLogConfig.getString(ProcessingLogConfig.TOPIC_NAME))
        .thenReturn(LOG_TOPIC_NAME);
    when(ksqlEngine.createSandbox(any())).thenReturn(sandBox);
    when(commandQueue.isEmpty()).thenReturn(true);
    when(commandQueue.enqueueCommand(any()))
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
        versionCheckerAgent,
        KsqlRestServiceContextBinder::new,
        securityExtension
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
  public void shouldCloseSecurityExtensionOnClose() {
    // When:
    app.stop();

    // Then:
    verify(securityExtension).close();
  }

  @Test
  public void shouldRegisterRestSecurityExtension() {
    // Given:
    final Configurable<?> configurable = mock(Configurable.class);

    // When:
    app.configureBaseApplication(configurable, Collections.emptyMap());

    // Then:
    verify(securityExtension).register(configurable, ksqlConfig);
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
    final PreparedStatement<?> statement =
        ProcessingLogServerUtils.processingLogStreamCreateStatement(
            processingLogConfig,
            ksqlConfig
        );
    verify(sandBox).execute(argThat(configured(equalTo(statement))));
    verify(commandQueue).enqueueCommand(
        argThat(configured(equalTo(statement), Collections.emptyMap(), ksqlConfig)));
  }

  @Test
  public void shouldNotCreateLogStreamIfAutoCreateNotConfigured() {
    // Given:
    when(processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE))
        .thenReturn(false);

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
    verify(commandQueue, never()).enqueueCommand(any());
  }

  @Test
  public void shouldNotCreateLogStreamIfValidationFails() {
    // Given:
    when(sandBox.execute(any())).thenThrow(new KsqlException("error"));

    // When:
    KsqlRestApplication.maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandQueue
    );

    // Then:
    verify(commandQueue, never()).enqueueCommand(any());
  }
}
