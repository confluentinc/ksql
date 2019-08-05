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
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectExecutorTest {

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final CreateConnector CREATE_CONNECTOR = new CreateConnector(
      "foo", ImmutableMap.of("foo", new StringLiteral("bar")), Type.SOURCE);

  private static final ConfiguredStatement<CreateConnector> CREATE_CONNECTOR_CONFIGURED =
      ConfiguredStatement.of(
          PreparedStatement.of(
              "CREATE SOURCE CONNECTOR foo WITH ('foo'='bar');",
              CREATE_CONNECTOR),
          ImmutableMap.of(),
          CONFIG);

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
    givenSuccess();

    // When:
    ConnectExecutor.execute(CREATE_CONNECTOR_CONFIGURED, null, serviceContext);

    // Then:
    verify(connectClient).create("foo", ImmutableMap.of("foo", "bar"));
  }

  @Test
  public void shouldReturnConnectorInfoEntityOnSuccess() {
    // Given:
    givenSuccess();

    // When:
    final Optional<KsqlEntity> entity = ConnectExecutor
        .execute(CREATE_CONNECTOR_CONFIGURED, null, serviceContext);

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(CreateConnectorEntity.class));
  }

  @Test
  public void shouldReturnErrorEntityOnError() {
    // Given:
    givenError();

    // When:
    final Optional<KsqlEntity> entity = ConnectExecutor
        .execute(CREATE_CONNECTOR_CONFIGURED, null, serviceContext);

    // Then:
    assertThat("Expected non-empty response", entity.isPresent());
    assertThat(entity.get(), instanceOf(ErrorEntity.class));
  }

  private void givenSuccess() {
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(ConnectResponse.of(
            new ConnectorInfo(
                "foo",
                ImmutableMap.of(),
                ImmutableList.of(),
                ConnectorType.SOURCE)));
  }

  private void givenError() {
    when(connectClient.create(anyString(), anyMap()))
        .thenReturn(ConnectResponse.of("error!"));
  }

}