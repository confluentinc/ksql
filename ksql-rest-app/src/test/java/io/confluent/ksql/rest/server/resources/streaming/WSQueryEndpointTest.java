package io.confluent.ksql.rest.server.resources.streaming;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryBody;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint.PrintTopicPublisher;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint.QueryPublisher;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.easymock.Capture;
import org.easymock.EasyMock;
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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final KsqlRequest VALID_REQUEST = new KsqlRequest("test-sql",
      ImmutableMap.of(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test-id"));

  private static final KsqlRequest ANOTHER_REQUEST = new KsqlRequest("other-sql",
      ImmutableMap.of());

  private static final String VALID_VERSION = Versions.KSQL_V1_WS;
  private static final String[] NO_VERSION_PROPERTY = null;
  private static final KsqlRequest[] NO_REQUEST_PROPERTY = (KsqlRequest[]) null;

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private StatementParser statementParser;
  @Mock
  private ListeningScheduledExecutorService exec;
  @Mock
  private Session session;
  @Mock
  private QueryBody queryBody;
  @Mock
  private QueryPublisher queryPublisher;
  @Mock
  private PrintTopicPublisher topicPublisher;
  @Captor
  private ArgumentCaptor<CloseReason> closeReasonCaptor;

  private Query query;
  private WSQueryEndpoint wsQueryEndpoint;
  private List mocks;

  @Before
  public void setUp() {
    query = new Query(queryBody, Optional.empty());

    when(session.getId()).thenReturn("session-id");
    when(statementParser.parseSingleStatement(anyString())).thenReturn(query);
    when(ksqlEngine.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    when(ksqlEngine.getTopicClient()).thenReturn(topicClient);
    givenRequest(VALID_REQUEST);

    wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfig, OBJECT_MAPPER, statementParser, ksqlEngine, exec,
        queryPublisher, topicPublisher, () -> {});
  }

  @Test
  public void shouldReturnErrorOnBadVersion() throws Exception {
    // Given:
    givenVersions("bad-version");

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Received invalid api version: [bad-version]");
  }

  @Test
  public void shouldReturnErrorOnMultipleVersions() throws Exception {
    // Given:
    givenVersions(Versions.KSQL_V1_WS, "2");

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Received multiple api versions: [1, 2]");
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
    verifyClosedWithReason("Error parsing request: missing request parameter");
  }

  @Test
  public void shouldReturnErrorOnEmptyRequests() throws Exception {
    // Given:
    givenRequests(/*None*/);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("Error parsing request: missing request parameter");
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
    verifyClosedWithReason("Error parsing query: Boom!");
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
    verifyClosedWithReason("Error parsing request: Failed to set 'unknown-property' to 'true'");
  }

  @Test
  public void shouldHandleQuery() {
    // Given:
    givenRequestIs(query);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(queryPublisher).start(
        eq(ksqlConfig),
        eq(ksqlEngine),
        eq(exec),
        eq(VALID_REQUEST.getKsql()),
        eq(VALID_REQUEST.getStreamsProperties()),
        any());
  }

  @Test
  public void shouldHandlePrintTopic() {
    // Given:
    givenRequestIs(printTopic("bob", true));
    when(topicClient.isTopicExists("bob")).thenReturn(true);
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(ImmutableMap.of("this", "that"));

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verify(topicPublisher).start(
        eq(exec),
        eq(schemaRegistryClient),
        eq(ImmutableMap.of("this", "that")),
        eq("bob"),
        eq(true),
        any());
  }

  @Test
  public void shouldReturnErrorIfTopicDoesNotExist() throws Exception {
    // Given:
    givenRequestIs(printTopic("bob", true));
    when(topicClient.isTopicExists("bob")).thenReturn(false);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    verifyClosedWithReason("topic does not exist: bob");
  }

  private PrintTopic printTopic(final String name, final boolean fromBeginning) {
    return new PrintTopic(
        new NodeLocation(0, 1),
        QualifiedName.of(name),
        fromBeginning,
        Optional.empty()
    );
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
    when(statementParser.parseSingleStatement(anyString())).thenReturn(stmt);
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

  private void verifyClosedWithReason(final String reason) throws Exception {
    verify(session).close(closeReasonCaptor.capture());
    final CloseReason closeReason = closeReasonCaptor.getValue();
    assertThat(closeReason.getReasonPhrase(), is(reason));
    assertThat(closeReason.getCloseCode(), is(CloseCodes.CANNOT_ACCEPT));
  }

  @Test
  public void shouldUpdateTheLastRequestTime() throws Exception {
    final ActivenessRegistrar activenessRegistrar = mock(ActivenessRegistrar.class);
    activenessRegistrar.updateLastRequestTime();
    expectLastCall();

    wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfig, OBJECT_MAPPER, statementParser, ksqlEngine, exec, activenessRegistrar);
    setupSessionMock();
    replay(activenessRegistrar);
    EasyMock.replay(activenessRegistrar);

    // When:
    wsQueryEndpoint.onOpen(session, null);

    // Then:
    EasyMock.verify(activenessRegistrar);
  }

  private void setupSessionMock() throws IOException {
    final Map<String, List<String>> parameters =
        Collections.singletonMap(
            Versions.KSQL_V1_WS_PARAM, Collections.singletonList("bad-version"));
    final CloseReason expectedCloseReason = new CloseReason(
        CloseReason.CloseCodes.CANNOT_ACCEPT,"Invalid version in request");
    final Capture<CloseReason> captured = Capture.newInstance();

    expect(session.getRequestParameterMap()).andReturn(parameters).anyTimes();
    expect(session.getId()).andReturn("session-id").anyTimes();
    session.close(capture(captured));
    expectLastCall().once();
  }

}
