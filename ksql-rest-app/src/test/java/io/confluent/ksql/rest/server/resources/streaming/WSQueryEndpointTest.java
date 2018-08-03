package io.confluent.ksql.rest.server.resources.streaming;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.websocket.CloseReason;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

public class WSQueryEndpointTest {
  private KsqlConfig ksqlConfig;
  private KsqlEngine ksqlEngine;
  private StatementParser statementParser;
  private ListeningScheduledExecutorService exec;
  private ObjectMapper objectMapper;
  private Session session;
  private WSQueryEndpoint wsQueryEndpoint;
  private List mocks;

  @Before
  public void setUp() {
    mocks = new LinkedList();
    ksqlConfig = addMock(KsqlConfig.class);
    ksqlEngine = addMock(KsqlEngine.class);
    statementParser = addMock(StatementParser.class);
    exec = addMock(ListeningScheduledExecutorService.class);
    objectMapper = new ObjectMapper();
    wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfig, objectMapper, statementParser, ksqlEngine, exec);
    session = addMock(Session.class);
  }

  private <T> T addMock(final Class<T> clazz) {
    final T mockObject = mock(clazz);
    mocks.add(mockObject);
    return mockObject;
  }

  private void replayMocks() {
    mocks.forEach(m -> replay(m));
  }

  private void verifyVersionCheckFailure(
      final CloseReason expectedCloseReason, final Capture<CloseReason> captured) {
    verify(session);
    final CloseReason closeReason = captured.getValue();
    assertThat(closeReason.getReasonPhrase(), equalTo(expectedCloseReason.getReasonPhrase()));
    assertThat(closeReason.getCloseCode(), equalTo(expectedCloseReason.getCloseCode()));
  }

  @Test
  public void shouldReturnErrorOnBadVersion() throws IOException {
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

    replayMocks();

    wsQueryEndpoint.onOpen(session, null);

    verifyVersionCheckFailure(expectedCloseReason, captured);
  }

  @Test
  public void shouldReturnErrorOnMultipleVersions() throws IOException {
    final Map<String, List<String>> parameters =
        Collections.singletonMap(
            Versions.KSQL_V1_WS_PARAM, Arrays.asList(
                Versions.KSQL_V1_WS, "2"));
    final CloseReason expectedCloseReason = new CloseReason(
        CloseReason.CloseCodes.CANNOT_ACCEPT,"Invalid version in request");
    final Capture<CloseReason> captured = Capture.newInstance();

    expect(session.getRequestParameterMap()).andReturn(parameters).anyTimes();
    expect(session.getId()).andReturn("session-id").anyTimes();
    session.close(capture(captured));
    expectLastCall().once();

    replayMocks();

    wsQueryEndpoint.onOpen(session, null);

    verifyVersionCheckFailure(expectedCloseReason, captured);
  }

  private void shouldReturnAllRows(final Map<String, List<String>> testParameters) throws IOException {
    final String statement = "ksql-query-statement";
    final Map<String, Object> properties = Collections.singletonMap("foo", "bar");
    final KsqlRequest request = new KsqlRequest(statement, properties);
    final Map<String, List<String>> parameters = new HashMap<>(testParameters);
    parameters.put(
        "request",
        Collections.singletonList(objectMapper.writeValueAsString(request)));
    final Schema schema = SchemaBuilder.struct()
        .field("f1", SchemaBuilder.int32())
        .field("f2", SchemaBuilder.string())
        .build();
    final List<KeyValue<String, GenericRow>> rows = new LinkedList<>();
    rows.add(new KeyValue<>("k1", new GenericRow("k1c1", "k2c2")));
    rows.add(new KeyValue<>("k2", new GenericRow("k2c1", "k2c2")));
    final BlockingQueue<KeyValue<String, GenericRow>> rowQ = new LinkedBlockingQueue<>(rows);

    final Query query = addMock(Query.class);
    final QueuedQueryMetadata queryMetadata = addMock(QueuedQueryMetadata.class);
    final KafkaStreams kafkaStreams = addMock(KafkaStreams.class);
    final  RemoteEndpoint.Async async = addMock(RemoteEndpoint.Async.class);
    final RemoteEndpoint.Basic basic = addMock(RemoteEndpoint.Basic.class);
    final ListenableScheduledFuture future = addMock(ListenableScheduledFuture.class);

    expect(session.getRequestParameterMap()).andReturn(parameters).anyTimes();
    expect(session.getBasicRemote()).andReturn(basic).anyTimes();
    expect(session.getAsyncRemote()).andReturn(async).anyTimes();
    expect(session.getId()).andReturn("session-id").anyTimes();

    expect(statementParser.parseSingleStatement(statement)).andReturn(query).anyTimes();

    expect(ksqlEngine.buildMultipleQueries(statement, ksqlConfig, properties))
        .andReturn(Collections.singletonList(queryMetadata))
        .anyTimes();

    expect(queryMetadata.getResultSchema()).andReturn(schema).anyTimes();
    queryMetadata.setLimitHandler(anyObject());
    expectLastCall().once();
    expect(queryMetadata.getKafkaStreams()).andReturn(kafkaStreams).anyTimes();
    expect(queryMetadata.getQueryApplicationId()).andReturn("foo").anyTimes();
    expect(queryMetadata.getRowQueue()).andReturn(rowQ).anyTimes();

    kafkaStreams.setUncaughtExceptionHandler(anyObject());
    expectLastCall().once();
    kafkaStreams.start();
    expectLastCall().once();

    final Capture<Runnable> captured = Capture.newInstance();
    expect(exec.submit(capture(captured))).andReturn(future).anyTimes();

    // result expectations
    basic.sendText(objectMapper.writeValueAsString(EntityUtil.buildSourceSchemaEntity(schema)));
    expectLastCall().once();
    for (final KeyValue<String, GenericRow> row : rows) {
      async.sendText(
          eq(objectMapper.writeValueAsString(StreamedRow.row(row.value))),
          anyObject());
      expectLastCall().once();
    }

    replayMocks();

    wsQueryEndpoint.onOpen(session, null);
    final Runnable queueHandler = captured.getValue();
    queueHandler.run();

    verify(ksqlEngine, statementParser, basic, async);
  }

  @Test
  public void shouldReturnAllRowsForNoVersion() throws IOException {
    shouldReturnAllRows(Collections.emptyMap());
  }

  @Test
  public void shouldReturnAllRowsExplicitVersion() throws IOException {
    shouldReturnAllRows(
        Collections.singletonMap(
            Versions.KSQL_V1_WS_PARAM, Collections.singletonList(Versions.KSQL_V1_WS)));
  }
}
