package io.confluent.ksql.rest.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingRequest;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.server.LagReportingAgent.Builder;
import io.confluent.ksql.rest.server.LagReportingAgent.HostPartitionLagInfo;
import io.confluent.ksql.rest.server.LagReportingAgent.SendLagService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
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
  private static long MAX_LAG_AGE_MS = 5000;
  private static long TIME_NOW_MS = 100;
  private static final String LOCALHOST_URL = "http://localhost:8088";
  private static HostInfoEntity LOCALHOST_INFO = new HostInfoEntity("localhost", 8088);

  private static HostInfoEntity HOST1 = new HostInfoEntity("host1", 1234);
  private static HostInfoEntity HOST2 = new HostInfoEntity("host2", 1234);

  private static Set<HostInfo> HOSTS = ImmutableSet.<HostInfo>builder()
      .add(new HostInfo("host1", 1234))
      .add(new HostInfo("host2", 1234))
      .build();

  private static final Map<String, Map<Integer, LagInfoEntity>> LAG_MAP1
      = ImmutableMap.<String, Map<Integer, LagInfoEntity>>builder()
      .put("a", ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(1, 10, 9))
          .put(3, new LagInfoEntity(3, 10, 7))
          .build())
      .put("b", ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(4, new LagInfoEntity(6, 10, 4))
          .build())
      .build();

  private static final Map<String, Map<Integer, LagInfoEntity>> LAG_MAP2
      = ImmutableMap.<String, Map<Integer, LagInfoEntity>>builder()
      .put("a", ImmutableMap.<Integer, LagInfoEntity>builder()
          .put(1, new LagInfoEntity(4, 10, 6))
          .build())
      .put("b", ImmutableMap.<Integer, LagInfoEntity>builder()
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
  @Mock
  private Ticker ticker;

  private LagReportingAgent lagReportingAgent;


  @Before
  public void setUp() {
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);

    Builder builder = LagReportingAgent.builder();
    lagReportingAgent = builder
        .lagDataExpirationMs(MAX_LAG_AGE_MS)
        .clock(clock)
        .ticker(ticker)
        .build(ksqlEngine, serviceContext);
    lagReportingAgent.setLocalAddress(LOCALHOST_URL);
  }

  @Test
  public void shouldReceiveLags() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST2, LAG_MAP2, 200));

    // Then:
    Map<HostInfoEntity, HostPartitionLagInfo> hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo("a", 1);
    assertEquals(2, hostPartitionLagList.size());
    assertEquals(1, hostPartitionLagList.get(HOST1).getLagInfo().getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HOST1).getLagInfo().getEndOffsetPosition());
    assertEquals(9, hostPartitionLagList.get(HOST1).getLagInfo().getOffsetLag());
    assertEquals(4, hostPartitionLagList.get(HOST2).getLagInfo().getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HOST2).getLagInfo().getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HOST2).getLagInfo().getOffsetLag());

    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo("a", 3);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(3, hostPartitionLagList.get(HOST1).getLagInfo().getCurrentOffsetPosition());

    hostPartitionLagList = lagReportingAgent.getHostsPartitionLagInfo("b", 4);
    assertEquals(2, hostPartitionLagList.size());
    assertEquals(6, hostPartitionLagList.get(HOST1).getLagInfo().getCurrentOffsetPosition());
    assertEquals(7, hostPartitionLagList.get(HOST2).getLagInfo().getCurrentOffsetPosition());
  }

  @Test
  public void shouldReceiveLags_expire() {
    // When:
    when(ticker.read()).thenReturn(1L);
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, TIME_NOW_MS));
    when(ticker.read()).thenReturn(Duration.ofMillis(MAX_LAG_AGE_MS + 100).toNanos());
    lagReportingAgent.receiveHostLag(hostLag(HOST2, LAG_MAP2, MAX_LAG_AGE_MS + 100));

    Map<HostInfoEntity, HostPartitionLagInfo> hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo("a", 1);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(4, hostPartitionLagList.get(HOST2).getLagInfo().getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HOST2).getLagInfo().getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HOST2).getLagInfo().getOffsetLag());
  }

  @Test
  public void shouldReceiveLags_removePreviousPartitions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP2, 200));

    // Then:
    Map<HostInfoEntity, HostPartitionLagInfo> hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo("a", 1);
    assertEquals(1, hostPartitionLagList.size());
    assertEquals(4, hostPartitionLagList.get(HOST1).getLagInfo().getCurrentOffsetPosition());
    assertEquals(10, hostPartitionLagList.get(HOST1).getLagInfo().getEndOffsetPosition());
    assertEquals(6, hostPartitionLagList.get(HOST1).getLagInfo().getOffsetLag());

    hostPartitionLagList
        = lagReportingAgent.getHostsPartitionLagInfo("a", 3);
    assertEquals(0, hostPartitionLagList.size());
  }

  @Test
  public void shouldReceiveLags_listAllCurrentPositions() {
    // When:
    lagReportingAgent.receiveHostLag(hostLag(HOST1, LAG_MAP1, 100));
    lagReportingAgent.receiveHostLag(hostLag(HOST2, LAG_MAP2, 200));

    // Then:
    Map<String, Map<Integer, Map<String, Long>>> allCurrentPositions
        = lagReportingAgent.listAllCurrentPositions();
    assertEquals("{a={1={host1,1234=1, host2,1234=4}, 3={host1,1234=3}}, "
        + "b={4={host1,1234=6, host2,1234=7}}}", allCurrentPositions.toString());
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
    Map<String, Map<Integer, LagInfo>> query0Lag
        = ImmutableMap.<String, Map<Integer, LagInfo>>builder()
        .put("a", ImmutableMap.<Integer, LagInfo>builder()
            .put(1, lagInfo0)
            .build())
        .build();
    Map<String, Map<Integer, LagInfo>> query1Lag
        = ImmutableMap.<String, Map<Integer, LagInfo>>builder()
        .put("b", ImmutableMap.<Integer, LagInfo>builder()
            .put(4, lagInfo1)
            .build())
        .build();

    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));
    when(query0.getStoreToPartitionToLagMap()).thenReturn(query0Lag);
    when(query1.getStoreToPartitionToLagMap()).thenReturn(query1Lag);
    lagReportingAgent.onClusterDiscoveryUpdate(HOSTS);
    SendLagService sendLagService = lagReportingAgent.new SendLagService();

    // When:
    sendLagService.runOneIteration();

    // Then:
    LagReportingRequest expected = new LagReportingRequest(LOCALHOST_INFO, LAG_MAP2, TIME_NOW_MS);
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host2:1234/")), eq(expected));
    verify(ksqlClient).makeAsyncLagReportRequest(eq(URI.create("http://host1:1234/")), eq(expected));
  }

  private LagReportingRequest hostLag(HostInfoEntity host,
                                Map<String, Map<Integer, LagInfoEntity>> lagMap,
                                long lastUpdateMs) {
    return new LagReportingRequest(host, lagMap, lastUpdateMs);
  }
}
