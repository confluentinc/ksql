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
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@RunWith(MockitoJUnitRunner.class)
public class ConnectExecutorTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final CreateConnector CREATE_CONNECTOR = new CreateConnector(
      "foo", ImmutableMap.of("connector.class",
        new StringLiteral("FileStreamSource")), Type.SOURCE, false);

  private static final ConfiguredStatement<CreateConnector> CREATE_CONNECTOR_CONFIGURED =
      ConfiguredStatement.of(PreparedStatement.of(
          "CREATE SOURCE CONNECTOR foo WITH ('connector.class'='FileStreamSource');",
          CREATE_CONNECTOR), SessionConfig.of(CONFIG, ImmutableMap.of()));

  private static final CreateConnector CREATE_DUPLICATE_CONNECTOR = new CreateConnector(
      "foo", ImmutableMap.of("connector.class",
        new StringLiteral("FileStreamSource")), Type.SOURCE,
      true);

  private static final ConfiguredStatement<CreateConnector> CREATE_DUPLICATE_CONNECTOR_CONFIGURED =
      ConfiguredStatement.of(PreparedStatement.of(
          "CREATE SOURCE CONNECTOR IF NOT EXISTS foo "
              + "WITH ('connector.class'='FileStreamSource');",
          CREATE_DUPLICATE_CONNECTOR), SessionConfig.of(CONFIG, ImmutableMap.of())
      );

  private static final ConnectExecutor EXECUTOR =
      new ConnectExecutor(new DefaultConnectServerErrors());

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;

  @Before
  public void setUp() {
    when(serviceContext.getConnectClient()).thenReturn(connectClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPassInCorrectArgsToConnectClient() {
    // Given:
    givenValidationSuccess();
    givenCreationSuccess();

    // When:
    EXECUTOR.execute(CREATE_CONNECTOR_CONFIGURED,
            mock(SessionProperties.class),
            null,
            serviceContext);

    // Then:
    verify(connectClient).validate(eq("FileStreamSource"),
        (Map<String, String>) and(
            argThat(hasEntry("connector.class", "FileStreamSource")),
            argThat(hasEntry("name", "foo"))));
    verify(connectClient).create(eq("foo"),
        (Map<String, String>) and(
            argThat(hasEntry("connector.class", "FileStreamSource")),
            argThat(hasEntry("name", "foo"))));
  }

  @Test
  public void shouldReturnConnectorInfoEntityOnSuccess() {
    // Given:
    givenValidationSuccess();
    givenCreationSuccess();

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(CREATE_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(CreateConnectorEntity.class));
  }

  @Test
  public void shouldReturnErrorEntityOnCreationError() {
    // Given:
    givenValidationSuccess();
    givenCreationError();

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(CREATE_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
  }

  @Test
  public void shouldReturnErrorEntityOnValidationError() {
    // Given:
    givenValidationError();
    givenCreationSuccess();

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(CREATE_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    final String expectedError = "Validation error: name - Name is missing\n"
        + "hostname - Hostname is required. Some other error";
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(), is(expectedError));
  }

  @Test
  public void shouldReturnWarningWhenIfNotExistsSetConnectorExists() {
    //Given:
    givenValidationSuccess();
    givenConnectorExists();

    //When
    final Optional<KsqlEntity> entity = EXECUTOR.execute(CREATE_DUPLICATE_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();
    //Then
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(WarningEntity.class));
  }

  @Test
  public void shouldReturnErrorIfConnectorExists() {
    //Given:
    givenValidationSuccess();
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(
            ConnectResponse.failure("Connector foo already exists", HttpStatus.SC_CONFLICT));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(CREATE_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
  }

  @Test
  public void shouldReturnErrorIfConnectorTypeIsMissing() {
    // Given:
    final CreateConnector createConnectorMissingType = new CreateConnector(
        "connector-name", ImmutableMap.of("foo", new StringLiteral("bar")),
        Type.SOURCE, false);
    final ConfiguredStatement<CreateConnector> createConnectorMissingTypeConfigured
        = ConfiguredStatement.of(PreparedStatement.of(
        "CREATE SOURCE CONNECTOR foo WITH ('foo'='bar');",
        createConnectorMissingType), SessionConfig.of(CONFIG, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(createConnectorMissingTypeConfigured,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    final String expectedError = "Validation error: "
        + "Connector config {name=connector-name, foo=bar} contains no connector type";
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(), is(expectedError));
  }

  @Test
  public void shouldReturnErrorIfConnectorTypeIsEmpty() {
    // Given:
    final CreateConnector createConnectorEmptyType = new CreateConnector(
        "foo", ImmutableMap.of("connector.class", new StringLiteral(" ")),
        Type.SOURCE, false);
    final ConfiguredStatement<CreateConnector> createConnectorEmptyTypeConfigured
        = ConfiguredStatement.of(PreparedStatement.of(
        "CREATE SOURCE CONNECTOR foo WITH ('connector.class'=' ');",
        createConnectorEmptyType), SessionConfig.of(CONFIG, ImmutableMap.of()));


    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(createConnectorEmptyTypeConfigured,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    final String expectedError = "Validation error: Connector type cannot be empty";
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(), is(expectedError));
  }

  @Test
  public void shouldReturnPluggableForbiddenError() {
    //Given:
    givenValidationSuccess();
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(
            ConnectResponse.failure("FORBIDDEN", HttpStatus.SC_FORBIDDEN));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new ConnectExecutor(connectErrorHandler)
        .execute(CREATE_CONNECTOR_CONFIGURED,
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
    givenValidationSuccess();
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(
            ConnectResponse.failure("UNAUTHORIZED", HttpStatus.SC_UNAUTHORIZED));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new ConnectExecutor(connectErrorHandler)
        .execute(CREATE_CONNECTOR_CONFIGURED,
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
    givenValidationSuccess();
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(
            ConnectResponse.failure("NOT ACCEPTABLE", HttpStatus.SC_NOT_ACCEPTABLE));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new ConnectExecutor(connectErrorHandler)
        .execute(CREATE_CONNECTOR_CONFIGURED,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat(((ErrorEntity) entity.get()).getErrorMessage(),
        is(DummyConnectServerErrors.DEFAULT_ERR));
  }

  private void givenCreationSuccess() {
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(ConnectResponse.success(
            new ConnectorInfo(
                "foo",
                ImmutableMap.of(),
                ImmutableList.of(),
                ConnectorType.SOURCE), HttpStatus.SC_OK));
  }

  private void givenValidationSuccess() {
    when(connectClient.validate(anyString(), anyMap()))
        .thenReturn(ConnectResponse.success(
            new ConfigInfos(
                "foo",
                0,
                ImmutableList.of(),
                ImmutableList.of()), HttpStatus.SC_OK));
  }

  private void givenValidationError() {
    final ConfigInfo configInfo1  = new ConfigInfo(new ConfigKeyInfo("name", "STRING",
        true, null, "HIGH", "docs",
        "Common", 1, "MEDIUM", "Connector name",
        ImmutableList.of()),
        new ConfigValueInfo("name", null, ImmutableList.of(), ImmutableList.of(
            "Name is missing"), true));
    final ConfigInfo configInfo2 = new ConfigInfo(new ConfigKeyInfo("hostname",
        "STRING", false, null, "HIGH",
        "docs for hostname",
        "Common", 2, "MEDIUM", "hostname",
        ImmutableList.of()),
        new ConfigValueInfo("hostname", null, ImmutableList.of(),
            ImmutableList.of("Hostname is required", "Some other error"), true));
    final ConfigInfo configInfo3 = new ConfigInfo(new ConfigKeyInfo("port",
        "INT", false, null, "HIGH",
        "docs for port",
        "Common", 3, "MEDIUM", "hostname",
        ImmutableList.of()),
        new ConfigValueInfo("port", null, ImmutableList.of(), ImmutableList.of(), true));
    when(connectClient.validate(anyString(), anyMap()))
        .thenReturn(ConnectResponse.success(
            new ConfigInfos(
                "foo",
                2,
                ImmutableList.of(),
                ImmutableList.of(configInfo1, configInfo2, configInfo3)),
            HttpStatus.SC_OK));
  }

  private void givenCreationError() {
    when(connectClient.validate(anyString(), anyMap()))
        .thenReturn(ConnectResponse.success(
            new ConfigInfos(
                "foo",
                0,
                ImmutableList.of(),
                ImmutableList.of()), HttpStatus.SC_OK));
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(ConnectResponse.failure("error!", HttpStatus.SC_BAD_REQUEST));
  }

  private void givenConnectorExists() {
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(
            Arrays.asList("foo", "bar"), HttpStatus.SC_OK));
  }

  private ConnectServerErrors givenCustomConnectErrorHandler() {
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KSQL_CONNECT_SERVER_ERROR_HANDLER, DummyConnectServerErrors.class));
    return config.getConfiguredInstance(
        KSQL_CONNECT_SERVER_ERROR_HANDLER,
        ConnectServerErrors.class);
  }
}