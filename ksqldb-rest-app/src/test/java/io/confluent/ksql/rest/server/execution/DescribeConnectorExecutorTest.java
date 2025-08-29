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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConfigInfos;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.IdentifierUtil;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hc.core5.http.HttpStatus;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorStateInfo;
import io.confluent.ksql.rest.entity.ConnectorStateInfo.ConnectorState;
import io.confluent.ksql.rest.entity.ConnectorStateInfo.TaskState;
import io.confluent.ksql.rest.entity.ConnectorType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DescribeConnectorExecutorTest {

  private static final String TOPIC = "kafka-topic";
  private static final String STATEMENT = "statement";
  private static final String CONNECTOR_NAME = "connector";
  private static final String CONNECTOR_CLASS = "io.confluent.ConnectorClazz";

  private static final ConnectorStateInfo STATUS = new ConnectorStateInfo(
      "connector",
      new ConnectorState("state", "worker", "msg"),
      ImmutableList.of(
          new TaskState(0, "state", "worker", "msg")),
      ConnectorType.SOURCE
    );

  private static final ConnectorInfo INFO = new ConnectorInfo(
      "connector",
      ImmutableMap.of(ConfigInfos.CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS),
      ImmutableList.of(),
      ConnectorType.SOURCE);

  private static Map<String, Map<String, List<String>>> ACTIVE_TOPICS = Collections.singletonMap(
      CONNECTOR_NAME, Collections.singletonMap(
          DescribeConnectorExecutor.TOPICS_KEY,
          Collections.singletonList(TOPIC)));

  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private MetaStore metaStore;
  @Mock
  private DataSource source;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;
  @Mock
  private Connector connector;

  private Function<ConnectorInfo, Optional<Connector>> connectorFactory;
  private DescribeConnectorExecutor executor;
  private ConfiguredStatement<DescribeConnector> describeStatement;

  @Before
  public void setUp() {
    final MetricCollectors metricCollectors = new MetricCollectors();
    when(engine.getMetaStore()).thenReturn(metaStore);
    when(engine.metricCollectors()).thenReturn(metricCollectors);
    when(serviceContext.getConnectClient()).thenReturn(connectClient);
    when(metaStore.getAllDataSources()).thenReturn(ImmutableMap.of(SourceName.of("source"), source));
    when(source.getKafkaTopicName()).thenReturn(TOPIC);
    when(source.getSqlExpression()).thenReturn(STATEMENT);
    when(source.getKsqlTopic()).thenReturn(
        new KsqlTopic(
            TOPIC,
            KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of()),
            ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of())
        )
    );
    when(source.getSchema()).thenReturn(
        LogicalSchema.builder()
            .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
            .valueColumn(ColumnName.of("foo"), SqlPrimitiveType.of( SqlBaseType.STRING))
            .build());
    when(source.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(source.getName()).thenReturn(SourceName.of("source"));
    when(connectClient.status(CONNECTOR_NAME)).thenReturn(ConnectResponse.success(STATUS, HttpStatus.SC_OK));
    when(connectClient.describe("connector")).thenReturn(ConnectResponse.success(INFO, HttpStatus.SC_OK));

    connectorFactory = info -> Optional.of(connector);
    executor = new DescribeConnectorExecutor(connectorFactory);

    final DescribeConnector describeConnector = new DescribeConnector(Optional.empty(), "connector");
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
    describeStatement = ConfiguredStatement
        .of(PreparedStatement.of("statementText", describeConnector),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));
  }

  @After
  public void teardown() {
    verifyNoMoreInteractions(
        engine, metaStore, connectClient);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldDescribeKnownConnector() {
    // Given:
    when(connectClient.topics("connector")).thenReturn(ConnectResponse.success(ACTIVE_TOPICS,
        HttpStatus.SC_OK));

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    verify(engine).getMetaStore();
    verify(engine).metricCollectors();
    verify(metaStore).getAllDataSources();
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");
    verify(connectClient).topics("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getConnectorClass(), is(CONNECTOR_CLASS));
    assertThat(description.getStatus(), is(STATUS));
    assertThat(description.getSources().size(), is(1));
    assertThat(description.getSources().get(0).getName(), is("source"));
    assertThat(description.getSources().get(0).getTopic(), is(TOPIC));
    assertThat(description.getTopics().size(), is(1));
    assertThat(description.getTopics().get(0), is(TOPIC));
  }

  @Test
  public void shouldDescribeKnownConnectorIfTopicListFails() {
    // Given:
    when(connectClient.topics("connector")).thenReturn(ConnectResponse.failure(
        "Topic tracking is disabled.", HttpStatus.SC_FORBIDDEN));

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    verify(engine).getMetaStore();
    verify(metaStore).getAllDataSources();
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");
    verify(connectClient).topics("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getConnectorClass(), is(CONNECTOR_CLASS));
    assertThat(description.getTopics().size(), is(0));
    assertThat(description.getWarnings().size(), is(1));
  }

  @Test
  public void shouldThrowIfConnectClientFailsStatus() {
    // Given:
    when(connectClient.status(any())).thenReturn(ConnectResponse.failure("error", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> executor.execute(describeStatement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    verify(connectClient).status("connector");

    assertThat(e.getResponse().getStatus(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    final KsqlErrorMessage err = (KsqlErrorMessage) e.getResponse().getEntity();
    assertThat(err.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    assertThat(err.getMessage(), containsString("Failed to query connector status: error"));
  }

  @Test
  public void shouldThrowIfConnectClientFailsDescribe() {
    // Given:
    when(connectClient.describe(any())).thenReturn(ConnectResponse.failure("error", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> executor.execute(describeStatement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");

    assertThat(e.getResponse().getStatus(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    final KsqlErrorMessage err = (KsqlErrorMessage) e.getResponse().getEntity();
    assertThat(err.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    assertThat(err.getMessage(), containsString("Failed to describe connector: error"));
  }

  @Test
  public void shouldNotWarnClientOnMissingTopicsEndpoint() {
    // Given:
    when(connectClient.topics(any())).thenReturn(ConnectResponse.failure("not found",
        HttpStatus.SC_NOT_FOUND));

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, mock(SessionProperties.class), engine, serviceContext)
        .getEntity();

    // Then:
    verify(engine).getMetaStore();
    verify(metaStore).getAllDataSources();
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");
    verify(connectClient).topics("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getTopics(), empty());
    assertThat(description.getWarnings(), empty());
  }

  @Test
  public void shouldWorkIfUnknownConnector() {
    // Given:
    connectorFactory = info -> Optional.empty();
    executor = new DescribeConnectorExecutor(connectorFactory);

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getConnectorClass(), is(CONNECTOR_CLASS));
    assertThat(description.getStatus(), is(STATUS));
    assertThat(description.getSources(), empty());
  }

}