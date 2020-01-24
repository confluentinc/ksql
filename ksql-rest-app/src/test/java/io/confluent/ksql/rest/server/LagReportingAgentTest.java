package io.confluent.ksql.rest.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.server.LagReportingAgent.Builder;
import io.confluent.ksql.rest.server.LagReportingAgent.SendLagService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
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
  private static Set<HostInfo> HOSTS = ImmutableSet.of(HI1, HI2);
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

  private static final QueryStateStoreId QUERY_STORE_A = QueryStateStoreId.of("query0", "a");
  private static final QueryStateStoreId QUERY_STORE_B = QueryStateStoreId.of("query1", "b");

  private static final Map<String, Map<Integer, LagInfoEntity>> LAG_MAP1
      = ImmutableMap.<String, Map<Integer, LagInfoEntity>>builder()
      .put(QUERY_STORE_A.toString(), ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(1, 10, 9))
          .put(3, new LagInfoEntity(3, 10, 7))
          .build())
      .put(QUERY_STORE_B.toString(), ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(6, 10, 4))
          .build())
      .build();

  private static final Map<String, Map<Integer, LagInfoEntity>> LAG_MAP2
      = ImmutableMap.<String, Map<Integer, LagInfoEntity>>builder()
      .put(QUERY_STORE_A.toString(), ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(4, 10, 6))
          .build())
      .put(QUERY_STORE_B.toString(), ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(7, 10, 3))
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
    Map<HostInfo, LagInfoEntity> hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo(HOSTS, QueryStateStoreId.of("query0", "a"), 1);
    assertEquals(2, hostPartitionLagList.size());
    assertEquals(1, hostPartitionLagList.get(HI1).getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HI1).getEndOffsetPosition());
    assertEquals(9, hostPartitionLagList.get(HI1).getOffsetLag());
    assertEquals(4, hostPartitionLagList.get(HI2).getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HI2).getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HI2).getOffsetLag());

    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo(HOSTS,
        QueryStateStoreId.of("query0", "a"), 3);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(3, hostPartitionLagList.get(HI1).getCurrentOffsetPosition());

    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo(HOSTS,
        QueryStateStoreId.of("query1", "b"), 4);
    assertEquals(2, hostPartitionLagList.size());
    assertEquals(6, hostPartitionLagList.get(HI1).getCurrentOffsetPosition());
    assertEquals(7, hostPartitionLagList.get(HI2).getCurrentOffsetPosition());

    lagReportingAgent.onHostStatusUpdated(HOSTS_HOST1_DEAD);
    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo(HOSTS,
        QueryStateStoreId.of("query0", "a"), 1);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(4, hostPartitionLagList.get(HI2).getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HI2).getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HI2).getOffsetLag());

    lagReportingAgent.onHostStatusUpdated(HOSTS_HOST2_DEAD);
    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo(HOSTS,
        QueryStateStoreId.of("query0", "a"), 1);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(1, hostPartitionLagList.get(HI1).getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HI1).getEndOffsetPosition());
    assertEquals(9, hostPartitionLagList.get(HI1).getOffsetLag());
  }

  @Test
  public void shouldReceiveLags_removePreviousPartitions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP2, 200));
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);

    // Then:
    Map<HostInfo, LagInfoEntity> hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo(HOSTS, QueryStateStoreId.of("query0", "a"), 1);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(4, hostPartitionLagList.get(HI1).getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HI1).getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HI1).getOffsetLag());

    hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo(HOSTS, QueryStateStoreId.of("query0", "a"), 3);
    assertEquals(0, hostPartitionLagList.size());
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
    assertEquals(1, lag.getCurrentOffsetPosition());
    assertEquals(10, lag.getEndOffsetPosition());
    assertEquals(9, lag.getOffsetLag());
    lag = allLags.get(HOST1).get(QUERY_STORE_A).get(3);
    assertEquals(3, lag.getCurrentOffsetPosition());
    assertEquals(10, lag.getEndOffsetPosition());
    assertEquals(7, lag.getOffsetLag());
    lag = allLags.get(HOST1).get(QUERY_STORE_B).get(4);
    assertEquals(6, lag.getCurrentOffsetPosition());
    assertEquals(10, lag.getEndOffsetPosition());
    assertEquals(4, lag.getOffsetLag());
    lag = allLags.get(HOST2).get(QUERY_STORE_A).get(1);
    assertEquals(4, lag.getCurrentOffsetPosition());
    assertEquals(10, lag.getEndOffsetPosition());
    assertEquals(6, lag.getOffsetLag());
    lag = allLags.get(HOST2).get(QUERY_STORE_B).get(4);
    assertEquals(7, lag.getCurrentOffsetPosition());
    assertEquals(10, lag.getEndOffsetPosition());
    assertEquals(3, lag.getOffsetLag());
  }

  @Test
  public void shouldSendLags() {
    // Given:
    when(clock.millis()).thenReturn(TIME_NOW_MS);
    when(lagInfo0.currentOffsetPosition()).thenReturn(4L);
    when(lagInfo0.endOffsetPosition()).thenReturn(10L);
    when(lagInfo0.offsetLag()).thenReturn(6L);
    when(lagInfo1.currentOffsetPosition()).thenReturn(7L);
    when(lagInfo1.endOffsetPosition()).thenReturn(10L);
    when(lagInfo1.offsetLag()).thenReturn(3L);
    Map<QueryStateStoreId, Map<Integer, LagInfo>> query0Lag
        = ImmutableMap.<QueryStateStoreId, Map<Integer, LagInfo>>builder()
        .put(QueryStateStoreId.of("query0", "a"), ImmutableMap.<Integer, LagInfo>builder()
            .put(1, lagInfo0)
            .build())
        .build();
    Map<QueryStateStoreId, Map<Integer, LagInfo>> query1Lag
        = ImmutableMap.<QueryStateStoreId, Map<Integer, LagInfo>>builder()
        .put(QueryStateStoreId.of("query1", "b"), ImmutableMap.<Integer, LagInfo>builder()
            .put(4, lagInfo1)
            .build())
        .build();

    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));
    when(query0.getStoreToPartitionToLagMap()).thenReturn(query0Lag);
    when(query1.getStoreToPartitionToLagMap()).thenReturn(query1Lag);
    SendLagService sendLagService = lagReportingAgent.new SendLagService();

    // When:
    lagReportingAgent.onHostStatusUpdated(HOSTS_ALIVE);
    sendLagService.runOneIteration();

    // Then:
    LagReportingRequest exp = new LagReportingRequest(LOCALHOST_INFO, LAG_MAP2, TIME_NOW_MS);
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host2:1234/")), eq(exp));
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), eq(exp));
  }

  private LagReportingRequest hostLag(HostInfoEntity host,
                                Map<String, Map<Integer, LagInfoEntity>> lagMap,
                                long lastUpdateMs) {
    return new LagReportingRequest(host, lagMap, lastUpdateMs);
  }
}
