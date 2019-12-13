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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.connect.Connector;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.ConnectorState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
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
      ImmutableMap.of(ConnectorConfig.CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS),
      ImmutableList.of(),
      ConnectorType.SOURCE);

  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private MetaStore metaStore;
  @Mock
  private DataSource<?> source;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;
  @Mock
  private Connector connector;

  @Mock
  private Admin adminClient;
  @Mock
  private ListTopicsResult topics;

  private Function<ConnectorInfo, Optional<Connector>> connectorFactory;
  private DescribeConnectorExecutor executor;
  private ConfiguredStatement<DescribeConnector> describeStatement;

  @Before
  public void setUp() {
    when(engine.getMetaStore()).thenReturn(metaStore);
    when(serviceContext.getConnectClient()).thenReturn(connectClient);
    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(metaStore.getAllDataSources()).thenReturn(ImmutableMap.of(SourceName.of("source"), source));
    when(source.getKafkaTopicName()).thenReturn(TOPIC);
    when(source.getSqlExpression()).thenReturn(STATEMENT);
    when(source.getKsqlTopic()).thenReturn(
        new KsqlTopic(
            TOPIC,
            KeyFormat.nonWindowed(FormatInfo.of(Format.AVRO)),
            ValueFormat.of(FormatInfo.of(Format.AVRO))
        )
    );
    when(source.getSchema()).thenReturn(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("foo"), SqlPrimitiveType.of( SqlBaseType.STRING))
            .build());
    when(source.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(source.getKeyField()).thenReturn(KeyField.none());
    when(source.getName()).thenReturn(SourceName.of("source"));
    when(connectClient.status(CONNECTOR_NAME)).thenReturn(ConnectResponse.success(STATUS, HttpStatus.SC_OK));
    when(connectClient.describe("connector")).thenReturn(ConnectResponse.success(INFO, HttpStatus.SC_OK));

    when(connector.matches(any())).thenReturn(false);
    when(connector.matches(TOPIC)).thenReturn(true);

    when(topics.names()).thenReturn(KafkaFuture.completedFuture(ImmutableSet.of(TOPIC, "other-topic")));
    when(adminClient.listTopics()).thenReturn(topics);

    connectorFactory = info -> Optional.of(connector);
    executor = new DescribeConnectorExecutor(connectorFactory);

    final DescribeConnector describeConnector = new DescribeConnector(Optional.empty(), "connector");
    describeStatement = ConfiguredStatement.of(
        PreparedStatement.of("statementText", describeConnector),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of()));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldDescribeKnownConnector() {
    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, ImmutableMap.of(), engine, serviceContext);

    // Then:
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
    KafkaFuture<Set<String>> fut = new KafkaFutureImpl<>();
    fut.cancel(true);
    when(topics.names()).thenReturn(fut);

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, ImmutableMap.of(), engine, serviceContext);

    // Then:
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getConnectorClass(), is(CONNECTOR_CLASS));
    assertThat(description.getTopics().size(), is(0));
    assertThat(description.getWarnings().size(), is(1));
  }

  @Test
  public void shouldErrorIfConnectClientFailsStatus() {
    // Given:
    when(connectClient.describe(any())).thenReturn(ConnectResponse.failure("error", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, ImmutableMap.of(), engine, serviceContext);

    // Then:
    verify(connectClient).status("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(), is("error"));
  }

  @Test
  public void shouldErrorIfConnectClientFailsDescribe() {
    // Given:
    when(connectClient.describe(any())).thenReturn(ConnectResponse.failure("error", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, ImmutableMap.of(), engine, serviceContext);

    // Then:
    verify(connectClient).status("connector");
    verify(connectClient).describe("connector");
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(), is("error"));
  }

  @Test
  public void shouldWorkIfUnknownConnector() {
    // Given:
    connectorFactory = info -> Optional.empty();
    executor = new DescribeConnectorExecutor(connectorFactory);

    // When:
    final Optional<KsqlEntity> entity = executor
        .execute(describeStatement, ImmutableMap.of(), engine, serviceContext);

    // Then:
    assertThat("Expected a response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ConnectorDescription.class));

    final ConnectorDescription description = (ConnectorDescription) entity.get();
    assertThat(description.getConnectorClass(), is(CONNECTOR_CLASS));
    assertThat(description.getStatus(), is(STATUS));
    assertThat(description.getSources(), empty());
  }

}