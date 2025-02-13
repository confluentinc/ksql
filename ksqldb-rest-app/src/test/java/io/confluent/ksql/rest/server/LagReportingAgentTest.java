package io.confluent.ksql.rest.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.LagReportingAgent.Builder;
import io.confluent.ksql.rest.server.LagReportingAgent.SendLagService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.LagInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LagReportingAgentTest {
  private static long TIME_NOW_MS = 100;
  private static final String LOCALHOST_URL = "http://localhost:8088";
  private static KsqlHostInfoEntity LOCALHOST_INFO = new KsqlHostInfoEntity("localhost", 8088);
  private static KsqlHostInfo HOST1 = new KsqlHostInfo("host1", 1234);
  private static KsqlHostInfo HOST2 = new KsqlHostInfo("host2", 1234);
  private static KsqlHostInfoEntity HOST_ENTITY1 = new KsqlHostInfoEntity("host1", 1234);
  private static KsqlHostInfoEntity HOST_ENTITY2 = new KsqlHostInfoEntity("host2", 1234);
  private static HostStatus HOST1_STATUS_ALIVE = new HostStatus(true, 0L);
  private static HostStatus HOST2_STATUS_ALIVE = new HostStatus(true, 0L);
  private static HostStatus HOST1_STATUS_DEAD = new HostStatus(false, 0L);
  private static HostStatus HOST2_STATUS_DEAD = new HostStatus(false, 0L);


  private static ImmutableMap<KsqlHostInfo, HostStatus> HOSTS_ALIVE
      = ImmutableMap.<KsqlHostInfo, HostStatus>builder()
      .put(HOST1, HOST1_STATUS_ALIVE)
      .put(HOST2, HOST2_STATUS_ALIVE)
      .build();

  private static ImmutableMap<KsqlHostInfo, HostStatus> HOSTS_HOST1_DEAD
      = ImmutableMap.<KsqlHostInfo, HostStatus>builder()
      .put(HOST1, HOST1_STATUS_DEAD)
      .put(HOST2, HOST2_STATUS_ALIVE)
      .build();

  private static ImmutableMap<KsqlHostInfo, HostStatus> HOSTS_HOST2_DEAD
      = ImmutableMap.<KsqlHostInfo, HostStatus>builder()
      .put(HOST1, HOST1_STATUS_ALIVE)
      .put(HOST2, HOST2_STATUS_DEAD)
      .build();

  private static final String QUERY_ID0 = "query0";
  private static final String QUERY_ID1 = "query1";
  private static final String STATE_STORE0 = "a";
  private static final String STATE_STORE1 = "b";
  private static final QueryStateStoreId QUERY_STORE_A =
      QueryStateStoreId.of(QUERY_ID0, STATE_STORE0);
  private static final QueryStateStoreId QUERY_STORE_B =
      QueryStateStoreId.of(QUERY_ID1, STATE_STORE1);

  private static final long M1_A1_CUR = 1;
  private static final long M1_A1_END = 10;
  private static final long M1_A1_LAG = 9;
  private static final long M1_A3_CUR = 3;
  private static final long M1_A3_END = 10;
  private static final long M1_A3_LAG = 7;
  private static final long M1_B4_CUR = 6;
  private static final long M1_B4_END = 10;
  private static final long M1_B4_LAG = 4;

  private static final ImmutableMap<QueryStateStoreId, StateStoreLags> LAG_MAP1
      = ImmutableMap.<QueryStateStoreId, StateStoreLags>builder()
      .put(QUERY_STORE_A, new StateStoreLags(ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(M1_A1_CUR, M1_A1_END, M1_A1_LAG))
          .put(3, new LagInfoEntity(M1_A3_CUR, M1_A3_END, M1_A3_LAG))
          .build()))
      .put(QUERY_STORE_B, new StateStoreLags(ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(M1_B4_CUR, M1_B4_END, M1_B4_LAG))
          .build()))
      .build();

  private static final long M2_A1_CUR = 4;
  private static final long M2_A1_END = 10;
  private static final long M2_A1_LAG = 6;
  private static final long M2_B4_CUR = 7;
  private static final long M2_B4_END = 10;
  private static final long M2_B4_LAG = 3;

  private static final ImmutableMap<QueryStateStoreId, StateStoreLags> LAG_MAP2
      = ImmutableMap.<QueryStateStoreId, StateStoreLags>builder()
      .put(QUERY_STORE_A, new StateStoreLags(ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(M2_A1_CUR, M2_A1_END, M2_A1_LAG))
          .build()))
      .put(QUERY_STORE_B, new StateStoreLags(ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(M2_B4_CUR, M2_B4_END, M2_B4_LAG))
          .build()))
      .build();

  @Mock
  private PersistentQueryMetadata query0;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private LagInfo lagInfo0;
  @Mock
  private LagInfo lagInfo1;
  @Mock
  private Clock clock;

  private LagReportingAgent lagReportingAgent;


  @Before
  public void setUp() {
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);

    Builder builder = LagReportingAgent.builder();
    lagReportingAgent = builder
        .clock(clock)
        .build(ksqlEngine, serviceContext);
    lagReportingAgent.setLocalAddress(LOCALHOST_URL);
  }

  @Test
  public void shouldReceiveLags() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY2, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Optional<LagInfoEntity> lagInfo
        = lagReportingAgent.getLagInfoForHost(HOST1, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M1_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M1_A1_LAG, lagInfo.get().getOffsetLag());
    lagInfo = lagReportingAgent.getLagInfoForHost(HOST2, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lagInfo.get().getOffsetLag());

    // Partition where just one of the hosts has lag data
    lagInfo = lagReportingAgent.getLagInfoForHost(HOST1, QUERY_STORE_A, 3);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_A3_CUR, lagInfo.get().getCurrentOffsetPosition());
    lagInfo = lagReportingAgent.getLagInfoForHost(HOST2, QUERY_STORE_A, 3);
    assertFalse(lagInfo.isPresent());

    // Second partition where they both have lag data
    lagInfo = lagReportingAgent.getLagInfoForHost(HOST1, QUERY_STORE_B, 4);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_B4_CUR, lagInfo.get().getCurrentOffsetPosition());
    lagInfo = lagReportingAgent.getLagInfoForHost(HOST2, QUERY_STORE_B, 4);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_B4_CUR, lagInfo.get().getCurrentOffsetPosition());
  }

  @Test
  public void shouldReceiveLags_removePreviousPartitions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY1, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Optional<LagInfoEntity> lagInfo
        = lagReportingAgent.getLagInfoForHost(HOST1, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lagInfo.get().getOffsetLag());

    lagInfo = lagReportingAgent.getLagInfoForHost(HOST1, QUERY_STORE_A, 3);
    assertFalse(lagInfo.isPresent());
  }

  @Test
  public void shouldReceiveLags_listAllCurrentPositions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST_ENTITY2, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    ImmutableMap<KsqlHostInfoEntity, HostStoreLags> allLags = lagReportingAgent.getAllLags();
    LagInfoEntity lag = Optional.ofNullable(allLags.get(HOST_ENTITY1))
        .orElseThrow(() -> new AssertionError("Lag could not be found"))
        .getStateStoreLags(QUERY_STORE_A)
        .flatMap(s -> s.getLagByPartition(1)).get();
    assertEquals(M1_A1_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_A1_END, lag.getEndOffsetPosition());
    assertEquals(M1_A1_LAG, lag.getOffsetLag());
    lag = Optional.ofNullable(allLags.get(HOST_ENTITY1))
        .orElseThrow(() -> new AssertionError("Lag could not be found"))
        .getStateStoreLags(QUERY_STORE_A)
        .flatMap(s -> s.getLagByPartition(3)).get();
    assertEquals(M1_A3_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_A3_END, lag.getEndOffsetPosition());
    assertEquals(M1_A3_LAG, lag.getOffsetLag());
    lag = Optional.ofNullable(allLags.get(HOST_ENTITY1))
        .orElseThrow(() -> new AssertionError("Lag could not be found"))
        .getStateStoreLags(QUERY_STORE_B)
        .flatMap(s -> s.getLagByPartition(4)).get();
    assertEquals(M1_B4_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_B4_END, lag.getEndOffsetPosition());
    assertEquals(M1_B4_LAG, lag.getOffsetLag());
    lag = Optional.ofNullable(allLags.get(HOST_ENTITY2))
        .orElseThrow(() -> new AssertionError("Lag could not be found"))
        .getStateStoreLags(QUERY_STORE_A)
        .flatMap(s -> s.getLagByPartition(1)).get();
    assertEquals(M2_A1_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lag.getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lag.getOffsetLag());
    lag = Optional.ofNullable(allLags.get(HOST_ENTITY2))
        .orElseThrow(() -> new AssertionError("Lag could not be found"))
        .getStateStoreLags(QUERY_STORE_B)
        .flatMap(s -> s.getLagByPartition(4)).get();
    assertEquals(M2_B4_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M2_B4_END, lag.getEndOffsetPosition());
    assertEquals(M2_B4_LAG, lag.getOffsetLag());
  }

  private void setupLags() {
    when(clock.millis()).thenReturn(TIME_NOW_MS);
    when(lagInfo0.currentOffsetPosition()).thenReturn(M2_A1_CUR);
    when(lagInfo0.endOffsetPosition()).thenReturn(M2_A1_END);
    when(lagInfo0.offsetLag()).thenReturn(M2_A1_LAG);
    when(lagInfo1.currentOffsetPosition()).thenReturn(M2_B4_CUR);
    when(lagInfo1.endOffsetPosition()).thenReturn(M2_B4_END);
    when(lagInfo1.offsetLag()).thenReturn(M2_B4_LAG);
    Map<String, Map<Integer, LagInfo>> query0Lag
        = ImmutableMap.<String, Map<Integer, LagInfo>>builder()
        .put(STATE_STORE0, ImmutableMap.<Integer, LagInfo>builder()
            .put(1, lagInfo0)
            .build())
        .build();
    Map<String, Map<Integer, LagInfo>> query1Lag
        = ImmutableMap.<String, Map<Integer, LagInfo>>builder()
        .put(STATE_STORE1, ImmutableMap.<Integer, LagInfo>builder()
            .put(4, lagInfo1)
            .build())
        .build();

    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));
    when(query0.getAllLocalStorePartitionLags()).thenReturn(query0Lag);
    when(query0.getQueryId()).thenReturn(new QueryId(QUERY_ID0));
    when(query1.getAllLocalStorePartitionLags()).thenReturn(query1Lag);
    when(query1.getQueryId()).thenReturn(new QueryId(QUERY_ID1));
  }

  @Test
  public void shouldSendLags() {
    // Given:
    setupLags();
    SendLagService sendLagService = lagReportingAgent.new SendLagService();

    // When:
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);
    sendLagService.runOneIteration();

    // Then:
    LagReportingMessage exp = hostLag(LOCALHOST_INFO, LAG_MAP2, TIME_NOW_MS);
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host2:1234/")), eq(exp));
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), eq(exp));
  }

  @Test
  public void shouldSendLags_generalError() {
    // Given:
    ArrayList<PersistentQueryMetadata> queries = new ArrayList<>();
    queries.add(null);
    when(ksqlEngine.getPersistentQueries()).thenReturn(queries);
    SendLagService sendLagService = lagReportingAgent.new SendLagService();

    // When:
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);
    sendLagService.runOneIteration();

    // Then:
    verify(ksqlClient, never()).makeAsyncLagReportRequest(any(), any());
  }

  @Test
  public void shouldSendLags_networkError() {
    // Given:
    setupLags();
    SendLagService sendLagService = lagReportingAgent.new SendLagService();
    doThrow(new RuntimeException("Error!"))
        .when(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), any());

    // When:
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);
    sendLagService.runOneIteration();

    // Then:
    LagReportingMessage exp = hostLag(LOCALHOST_INFO, LAG_MAP2, TIME_NOW_MS);
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host2:1234/")), eq(exp));
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), eq(exp));
  }

  private LagReportingMessage hostLag(
      KsqlHostInfoEntity host,
      ImmutableMap<QueryStateStoreId, StateStoreLags> lagMap,
      long lastUpdateMs) {
    return new LagReportingMessage(host, new HostStoreLags(lagMap, lastUpdateMs));
  }
}
