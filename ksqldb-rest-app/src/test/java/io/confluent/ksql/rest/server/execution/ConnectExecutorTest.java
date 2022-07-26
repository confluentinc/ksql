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
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
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
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;

  @Before
  public void setUp() {
    when(serviceContext.getConnectClient()).thenReturn(connectClient);

    when(connectClient.connectors()).thenReturn(
        ConnectResponse.success(ImmutableList.of(), HttpStatus.SC_OK));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPassInCorrectArgsToConnectClientOnExecute() {
    // Given:
    givenCreationSuccess();

    // When:
    ConnectExecutor
        .execute(CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext);

    // Then:
    verify(connectClient).create(eq("foo"),
        (Map<String, String>) and(
            argThat(hasEntry("connector.class", "FileStreamSource")),
            argThat(hasEntry("name", "foo"))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPassInCorrectArgsToConnectClientOnValidate() {
    // Given:
    givenValidationSuccess();

    // When:
    ConnectExecutor
        .validate(CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext);

    // Then:
    verify(connectClient).validate(eq("FileStreamSource"),
        (Map<String, String>) and(
            argThat(hasEntry("connector.class", "FileStreamSource")),
            argThat(hasEntry("name", "foo"))));
  }

  @Test
  public void shouldReturnConnectorInfoEntityOnSuccess() {
    // Given:
    givenCreationSuccess();

    // When:
    final Optional<KsqlEntity> entity = ConnectExecutor
        .execute(CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(CreateConnectorEntity.class));
  }

  @Test
  public void shouldThrowOnCreationError() {
    // Given:
    givenCreationError();

    // When / Then:
    assertThrows(
        KsqlRestException.class,
        () -> ConnectExecutor.execute(
            CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext));
  }

  @Test
  public void shouldThrowOnValidationError() {
    // Given:
    givenValidationError();

    // When / Then:
    assertThrows(
        KsqlException.class,
        () -> ConnectExecutor.validate(
            CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext));
  }

  @Test
  public void shouldReturnWarningOnExecuteWhenIfNotExistsSetConnectorExists() {
    //Given:
    givenConnectorExists();

    //When
    final Optional<KsqlEntity> entity = ConnectExecutor
        .execute(CREATE_DUPLICATE_CONNECTOR_CONFIGURED,
            mock(SessionProperties.class), null, serviceContext).getEntity();
    //Then
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(WarningEntity.class));
  }

  @Test
  public void shouldThrowOnValidateIfConnectorExists() {
    // Given:
    givenConnectorExists();

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> ConnectExecutor.validate(CREATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(HttpStatus.SC_CONFLICT));
    final KsqlErrorMessage err = (KsqlErrorMessage) e.getResponse().getEntity();
    assertThat(err.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_CONFLICT)));
    assertThat(err.getMessage(), containsString("Connector foo already exists"));
  }

  @Test
  public void shouldNotThrowOnValidateWhenIfNotExistsSetConnectorExists() {
    // Given:
    givenConnectorExists();
    givenValidationSuccess();

    // When:
    ConnectExecutor.validate(CREATE_DUPLICATE_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext);

    // Then: did not throw
  }

  @Test
  public void shouldThrowIfConnectorTypeIsMissing() {
    // Given:
    final CreateConnector createConnectorMissingType = new CreateConnector(
        "connector-name", ImmutableMap.of("foo", new StringLiteral("bar")),
        Type.SOURCE, false);
    final ConfiguredStatement<CreateConnector> createConnectorMissingTypeConfigured
        = ConfiguredStatement.of(PreparedStatement.of(
        "CREATE SOURCE CONNECTOR foo WITH ('foo'='bar');",
        createConnectorMissingType), SessionConfig.of(CONFIG, ImmutableMap.of()));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> ConnectExecutor.validate(createConnectorMissingTypeConfigured, mock(SessionProperties.class),
            null, serviceContext));

    // Then:
    assertThat(e.getMessage(), is("Validation error: "
        + "Connector config {name=connector-name, foo=bar} contains no connector type"));
  }

  @Test
  public void shouldThrowIfConnectorTypeIsEmpty() {
    // Given:
    final CreateConnector createConnectorEmptyType = new CreateConnector(
        "foo", ImmutableMap.of("connector.class", new StringLiteral(" ")),
        Type.SOURCE, false);
    final ConfiguredStatement<CreateConnector> createConnectorEmptyTypeConfigured
        = ConfiguredStatement.of(PreparedStatement.of(
        "CREATE SOURCE CONNECTOR foo WITH ('connector.class'=' ');",
        createConnectorEmptyType), SessionConfig.of(CONFIG, ImmutableMap.of()));


    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> ConnectExecutor.validate(createConnectorEmptyTypeConfigured, mock(SessionProperties.class),
            null, serviceContext));

    // Then:
    assertThat(e.getMessage(), is("Validation error: Connector type cannot be empty"));
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
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(ConnectResponse.failure("error!", HttpStatus.SC_BAD_REQUEST));
  }

  private void givenConnectorExists() {
    when(connectClient.connectors())
        .thenReturn(ConnectResponse.success(
            Arrays.asList("foo", "bar"), HttpStatus.SC_OK));
  }
}