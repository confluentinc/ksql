package io.confluent.ksql.rest.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaximumLagFilterTest {

  private static KsqlHostInfo HOST = new KsqlHostInfo("host", 8088);
  private static KsqlHostInfo HOST2 = new KsqlHostInfo("host2", 8088);
  private static List<KsqlHostInfo> HOSTS = ImmutableList.of(HOST, HOST2);
  private static String APPLICATION_ID = "app_id";
  private static String STATE_STORE = "state_store";
  private static int PARTITION = 3;
  private static LagInfoEntity LAG = new LagInfoEntity(3, 12, 9);
  private static LagInfoEntity LAG2 = new LagInfoEntity(7, 15, 8);

  @Mock
  private LagReportingAgent lagReportingAgent;
  @Mock
  private RoutingOptions routingOptions;

  private MaximumLagFilter filter;

  @Before
  public void setUp() {
    when(lagReportingAgent.getHostsPartitionLagInfo(eq(HOST),
        eq(QueryStateStoreId.of(APPLICATION_ID, STATE_STORE)), eq(PARTITION)))
        .thenReturn(Optional.of(LAG));
    when(lagReportingAgent.getHostsPartitionLagInfo(eq(HOST2),
        eq(QueryStateStoreId.of(APPLICATION_ID, STATE_STORE)), eq(PARTITION)))
        .thenReturn(Optional.of(LAG2));
  }


  @Test
  public void filter_shouldIncludeBelowThreshold() {
    // The max end offset is 15, so the lag for HOST is 12

    // Given:
    when(routingOptions.getOffsetLagAllowed()).thenReturn(13L);

    // When:
    filter = MaximumLagFilter.create(
        Optional.of(lagReportingAgent), routingOptions, HOSTS, APPLICATION_ID, STATE_STORE,
        PARTITION).get();

    // Then:
    assertTrue(filter.filter(HOST));
  }

  @Test
  public void filter_shouldNotIncludeAboveThreshold() {
    // The max end offset is 15, so the lag for HOST is 12

    // Given:
    when(routingOptions.getOffsetLagAllowed()).thenReturn(11L);

    // When:
    filter = MaximumLagFilter.create(
        Optional.of(lagReportingAgent), routingOptions, HOSTS, APPLICATION_ID, STATE_STORE,
        PARTITION).get();

    // Then:
    assertFalse(filter.filter(HOST));
  }

  @Test
  public void filter_hostNotReturned() {
    // Given:
    when(lagReportingAgent.getHostsPartitionLagInfo(eq(HOST),
        eq(QueryStateStoreId.of(APPLICATION_ID, STATE_STORE)), eq(PARTITION)))
        .thenReturn(Optional.empty());
    when(routingOptions.getOffsetLagAllowed()).thenReturn(13L);

    // When:
    filter = MaximumLagFilter.create(
        Optional.of(lagReportingAgent), routingOptions, HOSTS, APPLICATION_ID, STATE_STORE,
        PARTITION).get();

    // Then:
    assertTrue(filter.filter(HOST));
  }

  @Test
  public void filter_lagReportingDisabled() {
    // When:
    Optional<MaximumLagFilter> filterOptional = MaximumLagFilter.create(
        Optional.empty(), routingOptions, HOSTS, APPLICATION_ID, STATE_STORE, PARTITION);

    // Then:
    assertFalse(filterOptional.isPresent());
  }
}
