package io.confluent.ksql.rest.server.execution;


import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.Pair;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static org.easymock.EasyMock.anyInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class RemoteHostExecutorTest {
  @Mock
  public final KsqlEngine executionContext = mock(KsqlEngine.class);
  private final Set<HostInfo> hosts = Stream.of("otherhost:1234", "anotherhost:444")
      .map(HostInfo::buildFromEndpoint)
      .collect(Collectors.toSet());
  @Rule
  public TemporaryEngine engine = new TemporaryEngine();
  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private SessionProperties sessionProperties;
  @Mock
  private RestResponse<KsqlEntityList> response;
  @Mock
  private KsqlEntityList ksqlEntityList;
  private RemoteHostExecutor augmenter;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws MalformedURLException {

    when(sessionProperties.getInternalRequest()).thenReturn(false);
    when(sessionProperties.getLocalUrl()).thenReturn(new URL("https://address"));

    augmenter = RemoteHostExecutor.create(
        (ConfiguredStatement<DescribeStreams>) engine.configure("describe streams;"),
        sessionProperties,
        executionContext,
        ksqlClient);

  }

  @Test
  public void testReturnsHostsThatHaveThrownAnException() {
    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenThrow(new KsqlRestClientException("error"));
    try (MockedStatic<DiscoverRemoteHostsUtil> hdu = mockStatic(DiscoverRemoteHostsUtil.class)) {
      hdu.when(() -> DiscoverRemoteHostsUtil.getRemoteHosts(any(), any())).thenReturn(hosts);

      Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults = augmenter.fetchAllRemoteResults();
      assertEquals(hosts, remoteResults.getRight());
      assertThat(remoteResults.getLeft().entrySet(), hasSize(0));
    }
  }

  @Test
  public void testReturnsEmptyIfRequestIsInternal() {
    when(sessionProperties.getInternalRequest()).thenReturn(true);
    Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults = augmenter.fetchAllRemoteResults();
    assertThat(remoteResults.getLeft().entrySet(), hasSize(0));
    assertThat(remoteResults.getRight(), hasSize(0));
  }

  @Test
  public void testReturnsHostsThatHaveReturnedAnErroneousResponse() {
    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenReturn(response);
    when(response.isErroneous()).thenReturn(true);
    try (MockedStatic<DiscoverRemoteHostsUtil> hdu = mockStatic(DiscoverRemoteHostsUtil.class)) {
      hdu.when(() -> DiscoverRemoteHostsUtil.getRemoteHosts(any(), any())).thenReturn(hosts);

      Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults = augmenter.fetchAllRemoteResults();
      assertEquals(hosts, remoteResults.getRight());
      assertThat(remoteResults.getLeft().entrySet(), hasSize(0));
    }
  }

  @Test
  public void testReturnsRemoteResultsWhenEverythingIsFine() {
    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenReturn(response);
    when(response.isErroneous()).thenReturn(false);
    when(response.getResponse()).thenReturn(ksqlEntityList);
    when(ksqlEntityList.get(anyInt())).thenReturn(mock(KsqlEntity.class));
    try (MockedStatic<DiscoverRemoteHostsUtil> hdu = mockStatic(DiscoverRemoteHostsUtil.class)) {
      hdu.when(() -> DiscoverRemoteHostsUtil.getRemoteHosts(any(), any())).thenReturn(hosts);

      Pair<Map<HostInfo, KsqlEntity>, Set<HostInfo>> remoteResults = augmenter.fetchAllRemoteResults();
      assertThat(remoteResults.getRight(), is(empty()));
      assertEquals(remoteResults.getLeft().keySet(), hosts);
      assertThat(remoteResults.getLeft().entrySet(), hasSize(2));
    }
  }
}