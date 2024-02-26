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

import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.util.DiscoverRemoteHostsUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
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

@RunWith(MockitoJUnitRunner.class)
public class RemoteHostExecutorTest {
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
  @Mock
  private KsqlConfig ksqlConfig;
  private RemoteHostExecutor augmenter;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws MalformedURLException {
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    when(sessionProperties.getLocalUrl()).thenReturn(new URL("https://address"));

    augmenter = RemoteHostExecutor.create(
        (ConfiguredStatement<DescribeStreams>) engine.configure("describe streams;"),
        sessionProperties,
        engine.getEngine(),
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