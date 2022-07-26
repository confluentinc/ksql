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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
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
    DropConnectorExecutor.execute(DROP_CONNECTOR_CONFIGURED, mock(SessionProperties.class),null, serviceContext);

    // Then:
    verify(connectClient).delete("foo");
  }

  @Test
  public void shouldReturnOnSuccess() {
    // Given:
    when(connectClient.delete(anyString()))
        .thenReturn(ConnectResponse.success("foo", HttpStatus.SC_OK));

    // When:
    final Optional<KsqlEntity> response = DropConnectorExecutor
        .execute(DROP_CONNECTOR_CONFIGURED, mock(SessionProperties.class),null, serviceContext).getEntity();

    // Then:
    assertThat("expected response", response.isPresent());
    assertThat(((DropConnectorEntity) response.get()).getConnectorName(), is("foo"));
  }

  @Test
  public void shouldThrowOnError() {
    // Given:
    when(connectClient.delete(anyString()))
        .thenReturn(ConnectResponse.failure("Danger Mouse!", HttpStatus.SC_INTERNAL_SERVER_ERROR));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> DropConnectorExecutor.execute(
            DROP_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext));
    final KsqlRestException eIfExists = assertThrows(
        KsqlRestException.class,
        () -> DropConnectorExecutor.execute(
            DROP_CONNECTOR_CONFIGURED, mock(SessionProperties.class), null, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    final KsqlErrorMessage err = (KsqlErrorMessage) e.getResponse().getEntity();
    assertThat(err.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    assertThat(err.getMessage(), containsString("Failed to drop connector: Danger Mouse!"));

    assertThat(eIfExists.getResponse().getStatus(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    final KsqlErrorMessage errIfExists = (KsqlErrorMessage) e.getResponse().getEntity();
    assertThat(errIfExists.getErrorCode(), is(Errors.toErrorCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
    assertThat(errIfExists.getMessage(), containsString("Failed to drop connector: Danger Mouse!"));
  }

  @Test
  public void shouldReturnWarningIfNotExist() {
    // Given:
    when(connectClient.delete(anyString()))
            .thenReturn(ConnectResponse.failure("Danger Mouse!", HttpStatus.SC_NOT_FOUND));

    // When:
    final Optional<KsqlEntity> entity = DropConnectorExecutor
            .execute(DROP_CONNECTOR_IF_EXISTS_CONFIGURED, mock(SessionProperties.class), null, serviceContext).getEntity();

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(WarningEntity.class));
  }
}