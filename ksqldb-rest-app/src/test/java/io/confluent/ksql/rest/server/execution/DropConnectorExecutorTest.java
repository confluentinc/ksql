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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropConnectorExecutorTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final DropConnector DROP_CONNECTOR =
          new DropConnector(Optional.empty(), false, "foo");
  private static final DropConnector DROP_CONNECTOR_IF_EXISTS =
          new DropConnector(Optional.empty(), true, "foo");

  private static final ConfiguredStatement<DropConnector> DROP_CONNECTOR_CONFIGURED =
      ConfiguredStatement.of(PreparedStatement.of(
          "DROP CONNECTOR \"foo\"",
          DROP_CONNECTOR), SessionConfig.of(CONFIG, ImmutableMap.of()));
  private static final ConfiguredStatement<DropConnector> DROP_CONNECTOR_IF_EXISTS_CONFIGURED =
      ConfiguredStatement.of(PreparedStatement.of(
          "DROP CONNECTOR \"foo\"",
          DROP_CONNECTOR_IF_EXISTS), SessionConfig.of(CONFIG, ImmutableMap.of())
      );
  private static final DropConnectorExecutor EXECUTOR = new DropConnectorExecutor(
      new DefaultConnectServerErrors());

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConnectClient connectClient;

  @Before
  public void setUp() {
    when(serviceContext.getConnectClient()).thenReturn(connectClient);
  }


  @Test
  public void shouldPassInCorrectArgsToConnectClient() {
    // Given:
    when(connectClient.delete(anyString()))
        .thenReturn(ConnectResponse.success("foo", HttpStatus.SC_OK));

    // When:
    EXECUTOR.execute(DROP_CONNECTOR_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext);

    // Then:
    verify(connectClient).delete("foo");
  }

  @Test
  public void shouldReturnOnSuccess() {
    // Given:
    when(connectClient.delete(anyString()))
        .thenReturn(ConnectResponse.success("foo", HttpStatus.SC_OK));

    // When:
    final Optional<KsqlEntity> response = EXECUTOR.execute(DROP_CONNECTOR_CONFIGURED,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();

    // Then:
    assertThat("expected response", response.isPresent());
    assertThat(((DropConnectorEntity) response.get()).getConnectorName(), is("foo"));
  }

  @Test
  public void shouldReturnErrorEntityOnError() {
    // Given:
    when(connectClient.delete(anyString()))
        .thenReturn(ConnectResponse.failure("Danger Mouse!", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(DROP_CONNECTOR_CONFIGURED,
            mock(SessionProperties.class),
            null,
            serviceContext).getEntity();
    final Optional<KsqlEntity> entityIfExists = EXECUTOR
            .execute(DROP_CONNECTOR_IF_EXISTS_CONFIGURED,
                mock(SessionProperties.class),
                null,
                serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
    assertThat("Expected non-empty response", entityIfExists.isPresent());
    assertThat(entityIfExists.get(), instanceOf(ErrorEntity.class));
  }

  @Test
  public void shouldReturnWarningIfNotExist() {
    // Given:
    when(connectClient.delete(anyString()))
            .thenReturn(ConnectResponse.failure("Danger Mouse!", HttpStatus.SC_NOT_FOUND));

    // When:
    final Optional<KsqlEntity> entity = EXECUTOR.execute(DROP_CONNECTOR_IF_EXISTS_CONFIGURED,
        mock(SessionProperties.class),
        null,
        serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(WarningEntity.class));
  }

  @Test
  public void shouldReturnPluggableForbiddenError() {
    //Given:
    when(connectClient.delete(anyString()))
        .thenReturn(
            ConnectResponse.failure("FORBIDDEN", HttpStatus.SC_FORBIDDEN));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new DropConnectorExecutor(connectErrorHandler)
        .execute(DROP_CONNECTOR_CONFIGURED,
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
    when(connectClient.delete(anyString()))
        .thenReturn(
            ConnectResponse.failure("UNAUTHORIZED", HttpStatus.SC_UNAUTHORIZED));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new DropConnectorExecutor(connectErrorHandler)
        .execute(DROP_CONNECTOR_CONFIGURED,
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
    when(connectClient.delete(anyString()))
        .thenReturn(
            ConnectResponse.failure("NOT ACCEPTABLE", HttpStatus.SC_NOT_ACCEPTABLE));
    final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();

    // When:
    final Optional<KsqlEntity> entity = new DropConnectorExecutor(connectErrorHandler)
        .execute(DROP_CONNECTOR_CONFIGURED,
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