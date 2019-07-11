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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.TopicAccessValidator;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.context.ConfiguredKafkaClientSupplier;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint.PrintTopicPublisher;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint.QueryPublisher;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint.UserServiceContextFactory;
import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.rest.server.security.KsqlUserContextProvider;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.IOException;
import java.security.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"unchecked", "SameParameterValue"})
@RunWith(MockitoJUnitRunner.class)
public class WSQueryEndpointTest {

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  private static final KsqlRequest VALID_REQUEST = new KsqlRequest("test-sql",
      ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), null);

  private static final KsqlRequest ANOTHER_REQUEST = new KsqlRequest("other-sql",
      ImmutableMap.of(), null);

  private static final long SEQUENCE_NUMBER = 2L;
  private static final KsqlRequest REQUEST_WITHOUT_SEQUENCE_NUMBER = VALID_REQUEST;
  private static final KsqlRequest REQUEST_WITH_SEQUENCE_NUMBER = new KsqlRequest("test-sql",
      ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), SEQUENCE_NUMBER);

  private static final String VALID_VERSION = Versions.KSQL_V1_WS;
  private static final String[] NO_VERSION_PROPERTY = null;
  private static final KsqlRequest[] NO_REQUEST_PROPERTY = (KsqlRequest[]) null;
  private static final Duration COMMAND_QUEUE_CATCHUP_TIMEOUT = Duration.ofMillis(5000L);

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private Supplier<SchemaRegistryClient> schemaRegistryClientSupplier;
  @Mock
  private ConfiguredKafkaClientSupplier topicClientSupplier;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private Principal principal;
  @Mock
  private StatementParser statementParser;
  @Mock
  private ListeningScheduledExecutorService exec;
  @Mock
  private Session session;
  @Mock
  private CommandQueue commandQueue;
  @Mock
  private QueryPublisher queryPublisher;
  @Mock
  private PrintTopicPublisher topicPublisher;
  @Mock
  private ActivenessRegistrar activenessRegistrar;
  @Mock
  private TopicAccessValidator topicAccessValidator;
  @Mock
  private KsqlSecurityExtension securityExtension;
  @Mock
  private KsqlAuthorizationProvider authorizationProvider;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private UserServiceContextFactory serviceContextFactory;
  @Mock
  private ServerState serverState;
  @Mock
  private KsqlUserContextProvider userContextProvider;
  @Mock
  private Function<KsqlConfig, ServiceContext> defaultServiceContextProvider;
  @Captor
  private ArgumentCaptor<CloseReason> closeReasonCaptor;
  private Query query;
  private WSQueryEndpoint wsQueryEndpoint;

  @Before
  public void setUp() {
    query = new Query(
      mock(Select.class), mock(Relation.class),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), OptionalInt.empty()
    );
    when(session.getId()).thenReturn("session-id");
    when(session.getUserPrincipal()).thenReturn(principal);
    when(statementParser.parseSingleStatement(anyString()))
        .thenAnswer(invocation -> PreparedStatement.of(invocation.getArgument(0).toString(), query));
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));
    when(userContextProvider.getSchemaRegistryClientFactory(any()))
        .thenReturn(schemaRegistryClientSupplier);
    when(userContextProvider.getKafkaClientSupplier(any()))
        .thenReturn(topicClientSupplier);
    when(securityExtension.getAuthorizationProvider())
        .thenReturn(Optional.of(authorizationProvider));
    when(serviceContextFactory.create(ksqlConfig, topicClientSupplier, schemaRegistryClientSupplier))
        .thenReturn(serviceContext);
    when(defaultServiceContextProvider.apply(ksqlConfig)).thenReturn(serviceContext);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(serverState.checkReady()).thenReturn(Optional.empty());
    givenRequest(VALID_REQUEST);

    wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfig, OBJECT_MAPPER, statementParser, ksqlEngine, commandQueue, exec,
        queryPublisher, topicPublisher, activenessRegistrar, COMMAND_QUEUE_CATCHUP_TIMEOUT,
        topicAccessValidator, securityExtension, serviceContextFactory,
        defaultServiceContextProvider, serverState);
  }

  @Test
  public void shouldReturnErrorOnBadVersion() throws Exception {
    // Given:
    givenVersions("bad-version");

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Received invalid api version: [bad-version]", CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldReturnErrorOnMultipleVersions() throws Exception {
    // Given:
    givenVersions(Versions.KSQL_V1_WS, "2");

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Received multiple api versions: [1, 2]", CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldAcceptNoVersion() throws IOException {
    // Given:
    givenVersions(NO_VERSION_PROPERTY);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(session, never()).close(any());
  }

  @Test
  public void shouldAcceptEmptyVersions() throws IOException {
    // Given:
    givenVersions(/* empty version*/);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(session, never()).close(any());
  }

  @Test
  public void shouldAcceptExplicitVersion() throws IOException {
    // Given:
    givenVersions(Versions.KSQL_V1_WS);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(session, never()).close(any());
  }

  @Test
  public void shouldReturnErrorOnNoRequest() throws Exception {
    // Given:
    givenRequests(NO_REQUEST_PROPERTY);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason(
        "Error parsing request: missing request parameter", CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldReturnErrorOnEmptyRequests() throws Exception {
    // Given:
    givenRequests(/*None*/);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason(
        "Error parsing request: missing request parameter", CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldParseStatementText() {
    // Given:
    givenVersions(VALID_VERSION);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(statementParser).parseSingleStatement(VALID_REQUEST.getKsql());
  }

  @Test
  public void shouldWeirdlyIgnoreAllButTheLastRequest() {
    // Given:
    givenRequests(ANOTHER_REQUEST, VALID_REQUEST);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(statementParser).parseSingleStatement(VALID_REQUEST.getKsql());
  }

  @Test
  public void shouldReturnErrorOnBadStatement() throws Exception {
    // Given:
    when(statementParser.parseSingleStatement(anyString()))
        .thenThrow(new RuntimeException("Boom!"));

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Error parsing query: Boom!", CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldReturnErrorOnInvalidStreamProperty() throws Exception {
    // Given:
    final String jsonRequest = "{"
        + "\"ksql\":\"sql\","
        + "\"streamsProperties\":{"
        + "\"unknown-property\":true"
        + "}}";

    givenRequest(jsonRequest);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason(
        "Error parsing request: Failed to set 'unknown-property' to 'true'",
        CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldReturnErrorOnFailedStateCheck() throws Exception {
    // Given:
    when(serverState.checkReady()).thenReturn(
        Optional.of(
            Response.status(503)
                .entity(new KsqlErrorMessage(50300, "error"))
                .build()
        )
    );
    givenRequest("");

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("error", CloseCodes.TRY_AGAIN_LATER);
  }

  @Test
  public void shouldHandleQuery() {
    // Given:
    givenRequestIs(query);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    final ConfiguredStatement<Query> configuredStatement = ConfiguredStatement.of(
        PreparedStatement.of(VALID_REQUEST.getKsql(), query),
        VALID_REQUEST.getStreamsProperties(),
        ksqlConfig);

    verify(queryPublisher).start(
        eq(ksqlEngine),
        eq(serviceContext),
        eq(exec),
        eq(configuredStatement),
        any());
  }

  @Test
  public void shouldHandlePrintTopic() {
    // Given:
    givenRequestIs(StreamingTestUtils.printTopic("bob", true, null, null));
    when(topicClient.isTopicExists("bob")).thenReturn(true);
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(ImmutableMap.of("this", "that"));

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(topicPublisher).start(
        eq(exec),
        eq(serviceContext),
        eq(ImmutableMap.of("this", "that")),
        eq(StreamingTestUtils.printTopic("bob", true, null, null)),
        any());
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserContextProviderIsNotEnabled() {
    // Given:
    givenRequestIs(query);
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(defaultServiceContextProvider).apply(ksqlConfig);
    verifyZeroInteractions(userContextProvider);
  }

  @Test
  public void shouldCreateUserServiceContext() {
    // Given:
    givenRequestIs(query);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(userContextProvider).getKafkaClientSupplier(eq(principal));
    verify(userContextProvider).getSchemaRegistryClientFactory(eq(principal));
    verify(serviceContextFactory).create(
        ksqlConfig,
        topicClientSupplier,
        schemaRegistryClientSupplier
    );
    verifyZeroInteractions(defaultServiceContextProvider);
  }

  @Test
  public void shouldCloseUserServiceContextOnClose() {
    // Given:
    givenRequestIs(query);

    // When:
    wsQueryEndpoint.onOpen(session, null);
    wsQueryEndpoint.onClose(session, new CloseReason(CloseCodes.NO_STATUS_CODE, ""));

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldReturnErrorIfTopicDoesNotExist() throws Exception {
    // Given:
    givenRequestIs(StreamingTestUtils.printTopic("bob", true, null, null));
    when(topicClient.isTopicExists("bob")).thenReturn(false);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason(
        "Topic does not exist, or KSQL does not have permission to list the topic: bob",
        CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldNotWaitIfNoSequenceNumberSpecified() throws Exception {
    // Given:
    givenRequest(REQUEST_WITHOUT_SEQUENCE_NUMBER);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfSequenceNumberSpecified() throws Exception {
    // Given:
    givenRequest(REQUEST_WITH_SEQUENCE_NUMBER);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(commandQueue).ensureConsumedPast(eq(SEQUENCE_NUMBER), any());
  }

  @Test
  public void shouldReturnErrorIfCommandQueueCatchupTimeout() throws Exception {
    // Given:
    givenRequest(REQUEST_WITH_SEQUENCE_NUMBER);
    doThrow(new TimeoutException("yikes"))
        .when(commandQueue).ensureConsumedPast(eq(SEQUENCE_NUMBER), any());

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("yikes", CloseCodes.TRY_AGAIN_LATER);
    verify(statementParser, never()).parseSingleStatement(any());
  }

  @Test
  public void shouldReturnErrorIfEndpointAuthorizationIsDenied() throws Exception {
    // Given:
    givenRequest(REQUEST_WITH_SEQUENCE_NUMBER);
    doThrow(new KsqlException("access denied")).when(authorizationProvider)
        .checkEndpointAccess(any(), eq("POST"), eq("/query"));

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("io.confluent.ksql.util.KsqlException: access denied",
        CloseCodes.CANNOT_ACCEPT);
  }

  @Test
  public void shouldCloseSessionOnError() throws Exception {
    // When:
    wsQueryEndpoint.onError(session, new Throwable("ahh scary"));

    // Then:
    verifyClosedWithReason("ahh scary", CloseCodes.UNEXPECTED_CONDITION);
  }

  private void givenVersions(final String... versions) {

    givenRequestAndVersions(
        Collections.singletonList(serialize(VALID_REQUEST)),
        versions == null ? null : Arrays.asList(versions));
  }

  private void givenRequest(final KsqlRequest request) {
    givenRequestAndVersions(
        Collections.singletonList(serialize(request)),
        ImmutableList.of(VALID_VERSION));
  }

  private void givenRequest(final String request) {
    givenRequestAndVersions(Collections.singletonList(request), ImmutableList.of(VALID_VERSION));
  }

  private void givenRequests(final KsqlRequest... requests) {
    if (requests == null) {
      givenRequestAndVersions(null, ImmutableList.of(VALID_VERSION));
      return;
    }

    givenRequestAndVersions(
        Arrays.stream(requests).map(WSQueryEndpointTest::serialize).collect(Collectors.toList()),
        ImmutableList.of(VALID_VERSION));
  }

  private void givenRequestAndVersions(final List<String> jsonRequest,
      final List<String> versions) {
    final Builder<String, List<String>> builder = ImmutableMap.builder();
    if (versions != null) {
      builder.put(Versions.KSQL_V1_WS_PARAM, versions);
    }
    if (jsonRequest != null) {
      builder.put("request", jsonRequest);
    }

    when(session.getRequestParameterMap()).thenReturn(builder.build());
  }

  private void givenRequestIs(final Statement stmt) {
    when(statementParser.parseSingleStatement(anyString()))
        .thenReturn(PreparedStatement.of("statement", stmt));
  }

  private static String serialize(final KsqlRequest request) {
    try {
      if (request == null) {
        return null;
      }
      return OBJECT_MAPPER.writeValueAsString(request);
    } catch (final Exception e) {
      throw new AssertionError("Invalid test", e);
    }
  }

  private void verifyClosedWithReason(final String reason, final CloseCodes code) throws Exception {
    verify(session).close(closeReasonCaptor.capture());
    final CloseReason closeReason = closeReasonCaptor.getValue();
    assertThat(closeReason.getReasonPhrase(), is(reason));
    assertThat(closeReason.getCloseCode(), is(code));
  }

  @Test
  public void shouldUpdateTheLastRequestTime() {
    // Given:

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(activenessRegistrar).updateLastRequestTime();
  }

}
