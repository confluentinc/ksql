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

import static io.confluent.ksql.util.KsqlConfig.KSQL_CONNECT_SERVER_ERROR_HANDLER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ListConnectors;
import io.confluent.ksql.parser.tree.ListConnectors.Scope;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SimpleConnectorInfo;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;
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

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class ListConnectorsExecutorTest {

  private static final String CONNECTOR_CLASS = "class";

  private static final ConnectorInfo INFO = new ConnectorInfo(
      "connector",
      ImmutableMap.of(ConnectorConfig.CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS),
      ImmutableList.of(),
      ConnectorType.SOURCE
  );

  private static final ConnectorStateInfo STATUS = new ConnectorStateInfo(
      "connector",
      new ConnectorState("RUNNING", "foo", "bar"),
      ImmutableList.of(
          new TaskState(0, "RUNNING", "", ""),
          new TaskState(1, "FAILED", "", "")
      ),
      ConnectorType.SOURCE
  );

  private static final ConnectorStateInfo STATUS_WARNING = new ConnectorStateInfo(
      "connector",
      new ConnectorState("RUNNING", "foo", "bar"),
      ImmutableList.of(
          new TaskState(0, "FAILED", "", ""),
          new TaskState(1, "FAILED", "", "")
      ),
      ConnectorType.SOURCE
  );

  private static final ListConnectorsExecutor EXECUTOR = new ListConnectorsExecutor(
      new DefaultConnectServerErrors());

  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;

  @Before
  public void setUp() {
    when(serviceContext.getConnectClient()).thenReturn(connectClient);
    when(connectClient.describe("connector"))
        .thenReturn(ConnectResponse.success(INFO, HttpStatus.SC_OK));
    when(connectClient.status("connector"))
        .thenReturn(ConnectResponse.success(STATUS, HttpStatus.SC_OK));
    when(connectClient.describe("connector2"))
        .thenReturn(ConnectResponse.failure("DANGER WILL ROBINSON.", HttpStatus.SC_NOT_FOUND));
  }

  @Test
  public void shouldListValidConnector() {
    // Given:
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(ImmutableList.of("connector"), HttpStatus.SC_OK));
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(statement,
        mock(SessionProperties.class),
        engine,
        serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    final ConnectorList connectorList = (ConnectorList) entity.get();

    assertThat(connectorList, is(new ConnectorList(
        "",
        ImmutableList.of(),
        ImmutableList.of(
            new SimpleConnectorInfo("connector", ConnectorType.SOURCE, CONNECTOR_CLASS, "RUNNING (1/2 tasks RUNNING)")
        )
    )));
  }

  @Test
  public void shouldLabelConnectorsWithNoRunningTasksAsWarning() {
    // Given:
    when(connectClient.status("connector"))
        .thenReturn(ConnectResponse.success(STATUS_WARNING, HttpStatus.SC_OK));
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(ImmutableList.of("connector"), HttpStatus.SC_OK));
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(statement,
        mock(SessionProperties.class),
        engine,
        serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    final ConnectorList connectorList = (ConnectorList) entity.get();

    assertThat(connectorList, is(new ConnectorList(
        "",
        ImmutableList.of(),
        ImmutableList.of(
            new SimpleConnectorInfo("connector", ConnectorType.SOURCE, CONNECTOR_CLASS, "WARNING (0/2 tasks RUNNING)")
        )
    )));
  }

  @Test
  public void shouldFilterNonMatchingConnectors() {
    // Given:
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(ImmutableList.of("connector", "connector2"),
            HttpStatus.SC_OK));
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.SINK)),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of())
        );

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(statement,
        mock(SessionProperties.class),
        engine,
        serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    final ConnectorList connectorList = (ConnectorList) entity.get();

    assertThat(connectorList, is(new ConnectorList(
        "",
        ImmutableList.of(),
        ImmutableList.of()
    )));
  }

  @Test
  public void shouldListInvalidConnectorWithNoInfo() {
    // Given:
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(ImmutableList.of("connector2"), HttpStatus.SC_OK));
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of())
        );

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(statement,
        mock(SessionProperties.class),
        engine,
        serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    final ConnectorList connectorList = (ConnectorList) entity.get();

    assertThat(connectorList, is(new ConnectorList(
        "",
        ImmutableList.of(
            new KsqlWarning("Could not describe connector connector2: DANGER WILL ROBINSON.")),
        ImmutableList.of(
            new SimpleConnectorInfo("connector2", ConnectorType.UNKNOWN, null, null)
        )
    )));
  }

  @Test
  public void shouldReturnPluggableForbiddenError() {
    //Given:
    when(connectClient.connectors())
        .thenReturn(
            ConnectResponse.failure("FORBIDDEN", HttpStatus.SC_FORBIDDEN));

    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = new ListConnectorsExecutor(connectErrorHandler)
        .execute(statement,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(),
        is(DummyConnectServerErrors.FORBIDDEN_ERR));
  }

  @Test
  public void shouldReturnPluggableUnauthorizedError() {
    //Given:
    when(connectClient.connectors())
        .thenReturn(
            ConnectResponse.failure("UNAUTHORIZED", HttpStatus.SC_UNAUTHORIZED));

    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = new ListConnectorsExecutor(connectErrorHandler)
        .execute(statement,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(),
        is(DummyConnectServerErrors.UNAUTHORIZED_ERR));
  }

  @Test
  public void shouldReturnDefaultPluggableErrorOnUnknownCode() {
    //Given:
    when(connectClient.connectors())
        .thenReturn(
            ConnectResponse.failure("NOT ACCEPTABLE", HttpStatus.SC_NOT_ACCEPTABLE));

    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
    final ConfiguredStatement<ListConnectors> statement = ConfiguredStatement
        .of(PreparedStatement.of("", new ListConnectors(Optional.empty(), Scope.ALL)),
            SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = new ListConnectorsExecutor(connectErrorHandler)
        .execute(statement,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(),
        is(DummyConnectServerErrors.DEFAULT_ERR));
  }

  private ConnectServerErrors givenCustomConnectErrorHandler() {
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KSQL_CONNECT_SERVER_ERROR_HANDLER, DummyConnectServerErrors.class));
    return config.getConfiguredInstance(
        KSQL_CONNECT_SERVER_ERROR_HANDLER,
        ConnectServerErrors.class);
  }

}