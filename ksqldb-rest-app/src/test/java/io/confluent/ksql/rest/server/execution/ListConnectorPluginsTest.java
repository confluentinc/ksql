/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.ListConnectorPlugins;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ConnectorPluginsList;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class ListConnectorPluginsTest {
    private static final PluginInfo INFO = new PluginInfo(
        "org.apache.kafka.connect.file.FileStreamSinkConnector",
        PluginType.SOURCE,
        "2.1"
    );

    private static final ListConnectorPluginsExecutor EXECUTOR =
        new ListConnectorPluginsExecutor(new DefaultConnectServerErrors());

    @Mock
    private KsqlExecutionContext engine;
    @Mock
    private ServiceContext serviceContext;
    @Mock
    private ConnectClient connectClient;

    @Before
    public void setUp() {
        when(serviceContext.getConnectClient()).thenReturn(connectClient);
        when(connectClient.connectorPlugins())
            .thenReturn(ConnectClient.ConnectResponse.success(
                Arrays.asList(INFO), HttpStatus.SC_OK));
    }

    @Test
    public void shouldListValidConnectorPlugins() {
        // Given:
        when(connectClient.connectorPlugins())
            .thenReturn(ConnectClient.ConnectResponse.success(ImmutableList.of(INFO), HttpStatus.SC_OK));
        final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());
        final ConfiguredStatement<ListConnectorPlugins> statement = ConfiguredStatement
            .of(KsqlParser.PreparedStatement.of("", new ListConnectorPlugins(Optional.empty())),
                SessionConfig.of(ksqlConfig, ImmutableMap.of()));

        // When:
        final Optional<KsqlEntity> entity = EXECUTOR.execute(statement,
            mock(SessionProperties.class),
            engine,
            serviceContext).getEntity();

        // Then:
        assertThat("expected response!", entity.isPresent());
        final ConnectorPluginsList connectorPluginsList = (ConnectorPluginsList) entity.get();

        assertThat(connectorPluginsList, is(new ConnectorPluginsList(
            "",
            ImmutableList.of(),
            ImmutableList.of(
                new SimpleConnectorPluginInfo(
                    "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    ConnectorType.SOURCE,
                    "2.1")
            )
        )));
    }

    @Test
    public void shouldReturnPluggableForbiddenError() {
        //Given:
        when(connectClient.connectorPlugins())
            .thenReturn(
                ConnectResponse.failure("FORBIDDEN", HttpStatus.SC_FORBIDDEN));

        final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
        final ConfiguredStatement<ListConnectorPlugins> statement = ConfiguredStatement
            .of(KsqlParser.PreparedStatement.of("", new ListConnectorPlugins(Optional.empty())),
                SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

        // When:
        final Optional<KsqlEntity> entity = new ListConnectorPluginsExecutor(connectErrorHandler)
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
        when(connectClient.connectorPlugins())
            .thenReturn(
                ConnectResponse.failure("UNAUTHORIZED", HttpStatus.SC_UNAUTHORIZED));

        final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
        final ConfiguredStatement<ListConnectorPlugins> statement = ConfiguredStatement
            .of(KsqlParser.PreparedStatement.of("", new ListConnectorPlugins(Optional.empty())),
                SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

        // When:
        final Optional<KsqlEntity> entity = new ListConnectorPluginsExecutor(connectErrorHandler)
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
        when(connectClient.connectorPlugins())
            .thenReturn(
                ConnectResponse.failure("NOT ACCEPTABLE", HttpStatus.SC_NOT_ACCEPTABLE));

        final ConnectServerErrors connectErrorHandler = givenCustomConnectErrorHandler();
        final ConfiguredStatement<ListConnectorPlugins> statement = ConfiguredStatement
            .of(KsqlParser.PreparedStatement.of("", new ListConnectorPlugins(Optional.empty())),
                SessionConfig.of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));

        // When:
        final Optional<KsqlEntity> entity = new ListConnectorPluginsExecutor(connectErrorHandler)
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
