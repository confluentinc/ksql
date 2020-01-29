package io.confluent.ksql.rest.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.server.LagReportingAgent.Builder;
import io.confluent.ksql.rest.server.LagReportingAgent.SendLagService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LagReportingAgentTest {
  private static long TIME_NOW_MS = 100;
  private static final String LOCALHOST_URL = "http://localhost:8088";
  private static HostInfoEntity LOCALHOST_INFO = new HostInfoEntity("localhost", 8088);

  private static HostInfoEntity HOST1 = new HostInfoEntity("host1", 1234);
  private static HostInfoEntity HOST2 = new HostInfoEntity("host2", 1234);
  private static HostInfo HI1 = new HostInfo("host1", 1234);
  private static HostInfo HI2 = new HostInfo("host2", 1234);
  private static HostStatusEntity HOST1_STATUS_ALIVE = new HostStatusEntity(HOST1, true, 0L);
  private static HostStatusEntity HOST2_STATUS_ALIVE = new HostStatusEntity(HOST2, true, 0L);
  private static HostStatusEntity HOST1_STATUS_DEAD = new HostStatusEntity(HOST1, false, 0L);
  private static HostStatusEntity HOST2_STATUS_DEAD = new HostStatusEntity(HOST2, false, 0L);

  private static Map<String, HostStatusEntity> HOSTS_ALIVE
      = ImmutableMap.<String, HostStatusEntity>builder()
      .put(HOST1.toString(), HOST1_STATUS_ALIVE)
      .put(HOST2.toString(), HOST2_STATUS_ALIVE)
      .build();

  private static Map<String, HostStatusEntity> HOSTS_HOST1_DEAD
      = ImmutableMap.<String, HostStatusEntity>builder()
      .put(HOST1.toString(), HOST1_STATUS_DEAD)
      .put(HOST2.toString(), HOST2_STATUS_ALIVE)
      .build();

  private static Map<String, HostStatusEntity> HOSTS_HOST2_DEAD
      = ImmutableMap.<String, HostStatusEntity>builder()
      .put(HOST1.toString(), HOST1_STATUS_ALIVE)
      .put(HOST2.toString(), HOST2_STATUS_DEAD)
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

  private static final Map<QueryStateStoreId, Map<Integer, LagInfoEntity>> LAG_MAP1
      = ImmutableMap.<QueryStateStoreId, Map<Integer, LagInfoEntity>>builder()
      .put(QUERY_STORE_A, ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(M1_A1_CUR, M1_A1_END, M1_A1_LAG))
          .put(3, new LagInfoEntity(M1_A3_CUR, M1_A3_END, M1_A3_LAG))
          .build())
      .put(QUERY_STORE_B, ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(M1_B4_CUR, M1_B4_END, M1_B4_LAG))
          .build())
      .build();

  private static final long M2_A1_CUR = 4;
  private static final long M2_A1_END = 10;
  private static final long M2_A1_LAG = 6;
  private static final long M2_B4_CUR = 7;
  private static final long M2_B4_END = 10;
  private static final long M2_B4_LAG = 3;

  private static final Map<QueryStateStoreId, Map<Integer, LagInfoEntity>> LAG_MAP2
      = ImmutableMap.<QueryStateStoreId, Map<Integer, LagInfoEntity>>builder()
      .put(QUERY_STORE_A, ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(M2_A1_CUR, M2_A1_END, M2_A1_LAG))
          .build())
      .put(QUERY_STORE_B, ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(M2_B4_CUR, M2_B4_END, M2_B4_LAG))
          .build())
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
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST2, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Optional<LagInfoEntity> lagInfo
        = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M1_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M1_A1_LAG, lagInfo.get().getOffsetLag());
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI2, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lagInfo.get().getOffsetLag());

    // Partition where just one of the hosts has lag data
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 3);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_A3_CUR, lagInfo.get().getCurrentOffsetPosition());
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI2, QUERY_STORE_A, 3);
    assertFalse(lagInfo.isPresent());

    // Second parition where they both have lag data
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_B, 4);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_B4_CUR, lagInfo.get().getCurrentOffsetPosition());
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI2, QUERY_STORE_B, 4);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_B4_CUR, lagInfo.get().getCurrentOffsetPosition());

    // Host 1 is dead
    lagReportingAgent.onHostStatusUpdated(HOSTS_HOST1_DEAD);
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 1);
    assertFalse(lagInfo.isPresent());
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI2, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lagInfo.get().getOffsetLag());

    // Host 2 is dead
    lagReportingAgent.onHostStatusUpdated(HOSTS_HOST2_DEAD);
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI2, QUERY_STORE_A, 1);
    assertFalse(lagInfo.isPresent());
    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M1_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M1_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M1_A1_LAG, lagInfo.get().getOffsetLag());
  }

  @Test
  public void shouldReceiveLags_removePreviousPartitions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Optional<LagInfoEntity> lagInfo
        = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 1);
    assertTrue(lagInfo.isPresent());
    assertEquals(M2_A1_CUR, lagInfo.get().getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lagInfo.get().getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lagInfo.get().getOffsetLag());

    lagInfo = lagReportingAgent.getHostsPartitionLagInfo(HI1, QUERY_STORE_A, 3);
    assertFalse(lagInfo.isPresent());
  }

  @Test
  public void shouldReceiveLags_listAllCurrentPositions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST2, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Map<HostInfoEntity, Map<QueryStateStoreId, Map<Integer, LagInfoEntity>>> allLags
        = lagReportingAgent.listAllLags();
    LagInfoEntity lag = allLags.get(HOST1).get(QUERY_STORE_A).get(1);
    assertEquals(M1_A1_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_A1_END, lag.getEndOffsetPosition());
    assertEquals(M1_A1_LAG, lag.getOffsetLag());
    lag = allLags.get(HOST1).get(QUERY_STORE_A).get(3);
    assertEquals(M1_A3_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_A3_END, lag.getEndOffsetPosition());
    assertEquals(M1_A3_LAG, lag.getOffsetLag());
    lag = allLags.get(HOST1).get(QUERY_STORE_B).get(4);
    assertEquals(M1_B4_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M1_B4_END, lag.getEndOffsetPosition());
    assertEquals(M1_B4_LAG, lag.getOffsetLag());
    lag = allLags.get(HOST2).get(QUERY_STORE_A).get(1);
    assertEquals(M2_A1_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M2_A1_END, lag.getEndOffsetPosition());
    assertEquals(M2_A1_LAG, lag.getOffsetLag());
    lag = allLags.get(HOST2).get(QUERY_STORE_B).get(4);
    assertEquals(M2_B4_CUR, lag.getCurrentOffsetPosition());
    assertEquals(M2_B4_END, lag.getEndOffsetPosition());
    assertEquals(M2_B4_LAG, lag.getOffsetLag());
  }

  @Test
  public void shouldSendLags() {
    // Given:
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
    when(query0.getQueryApplicationId()).thenReturn(QUERY_ID0);
    when(query1.getAllLocalStorePartitionLags()).thenReturn(query1Lag);
    when(query1.getQueryApplicationId()).thenReturn(QUERY_ID1);
    SendLagService sendLagService = lagReportingAgent.new SendLagService();

    // When:
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);
    sendLagService.runOneIteration();

    // Then:
    LagReportingMessage exp = new LagReportingMessage(LOCALHOST_INFO, LAG_MAP2, TIME_NOW_MS);
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host2:1234/")), eq(exp));
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), eq(exp));
  }

  private LagReportingMessage hostLag(
      HostInfoEntity host,
      Map<QueryStateStoreId, Map<Integer, LagInfoEntity>> lagMap,
      long lastUpdateMs) {
    return new LagReportingMessage(host, lagMap, lastUpdateMs);
  }
}
