/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsLocatorTest {

  private static final String APPLICATION_ID = "app_id";
  private static final String STORE_NAME = "someStoreName";
  private static final URL LOCAL_HOST_URL = localHost();
  private static final Struct SOME_KEY = new Struct(SchemaBuilder.struct().build());

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KeyQueryMetadata keyQueryMetadata;
  @Mock
  private Serializer<Struct> keySerializer;
  @Mock
  private RoutingFilter livenessFilter;
  @Mock
  private RoutingFilter activeFilter;
  @Mock
  private RoutingOptions routingOptions;

  private KsqlHostInfo activeHost;
  private KsqlHostInfo standByHost1;
  private KsqlHostInfo standByHost2;
  private HostInfo activeHostInfo;
  private HostInfo standByHostInfo1;
  private HostInfo standByHostInfo2;

  private KsLocator locator;
  private KsqlNode activeNode;
  private KsqlNode standByNode1;
  private KsqlNode standByNode2;
  private RoutingFilters routingStandbyFilters;
  private RoutingFilters routingActiveFilters;
  private RoutingFilterFactory routingFilterFactoryActive;
  private RoutingFilterFactory routingFilterFactoryStandby;
  private static final HostStatus HOST_ALIVE = new HostStatus(true, 0L);
  private static final HostStatus HOST_DEAD = new HostStatus(false, 0L);

  @Before
  public void setUp() {
    locator = new KsLocator(STORE_NAME, kafkaStreams, keySerializer, LOCAL_HOST_URL,
        APPLICATION_ID);

    activeHost = new KsqlHostInfo("remoteHost", 2345);
    activeHostInfo = new HostInfo("remoteHost", 2345);
    standByHost1 = new KsqlHostInfo("standby1", 1234);
    standByHostInfo1 = new HostInfo("standby1", 1234);
    standByHost2 = new KsqlHostInfo("standby2", 5678);
    standByHostInfo2 = new HostInfo("standby2", 5678);

    activeNode = locator.asNode(activeHost);
    standByNode1 = locator.asNode(standByHost1);
    standByNode2 = locator.asNode(standByHost2);

    routingStandbyFilters = new RoutingFilters(ImmutableList.of(livenessFilter));
    routingActiveFilters = new RoutingFilters(ImmutableList.of(activeFilter, livenessFilter));

    // Only active serves query
    when(activeFilter.filter(eq(activeHost)))
        .thenReturn(true);
    when(activeFilter.filter(eq(standByHost1)))
        .thenReturn(false);
    when(activeFilter.filter(eq(standByHost2)))
        .thenReturn(false);

    // Heartbeat not enabled, all hosts alive
    when(livenessFilter.filter(eq(activeHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(standByHost1)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(standByHost2)))
        .thenReturn(true);

    routingFilterFactoryActive = (routingOptions, hosts, active, applicationQueryId,
                                  storeName, partition) -> routingActiveFilters;
    routingFilterFactoryStandby = (routingOptions, hosts, active, applicationQueryId,
                                   storeName, partition) -> routingStandbyFilters;
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(URL.class, LOCAL_HOST_URL)
        .setDefault(KafkaStreams.class, kafkaStreams)
        .setDefault(Serializer.class, keySerializer)
        .testConstructors(KsLocator.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfMetadataNotAvailable() {
    // Given:
    getEmtpyMetadata();

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> locator.locate(SOME_KEY, routingOptions, routingFilterFactoryActive)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "KeyQueryMetadata not available for state store someStoreName and key Struct{}"));
  }

  @Test
  public void shouldReturnOwnerIfKnown() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    final Optional<URI> url = result.stream().findFirst().map(KsqlNode::location);
    assertThat(url.map(URI::getScheme), is(Optional.of(LOCAL_HOST_URL.getProtocol())));
    assertThat(url.map(URI::getHost), is(Optional.of(activeHost.host())));
    assertThat(url.map(URI::getPort), is(Optional.of(activeHost.port())));
    assertThat(url.map(URI::getPath), is(Optional.of("/")));
  }

  @Test
  public void shouldReturnLocalOwnerIfSameAsSuppliedLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo(LOCAL_HOST_URL.getHost(), LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(true);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnLocalOwnerIfExplicitlyLocalHostOnSamePortAsSuppliedLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("LocalHOST", LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(true);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("different", LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(true);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPort() {
    // Given:
    final HostInfo localHostInfo = new HostInfo(LOCAL_HOST_URL.getHost(), LOCAL_HOST_URL.getPort() + 1);
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(true);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPortOnLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("LOCALhost", LOCAL_HOST_URL.getPort() + 1);
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(true);
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(true);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnActiveWhenRoutingStandbyNotEnabledHeartBeatNotEnabled() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryActive);

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result.stream().findFirst().get(), is(activeNode));
  }

  @Test
  public void shouldReturnActiveAndStandBysWhenRoutingStandbyEnabledHeartBeatNotEnabled() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryStandby);

    // Then:
    assertThat(result.size(), is(3));
    assertThat(result.stream().findFirst().get(), is(activeNode));
    assertThat(result, containsInAnyOrder(activeNode, standByNode1, standByNode2));
  }

  @Test
  public void shouldReturnStandBysWhenActiveDown() {
    // Given:
    getActiveAndStandbyMetadata();
    when(livenessFilter.filter(eq(activeHost)))
        .thenReturn(false);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryStandby);

    // Then:
    assertThat(result.size(), is(2));
    assertThat(result, containsInAnyOrder(standByNode1, standByNode2));
  }

  @Test
  public void shouldReturnOneStandByWhenActiveAndOtherStandByDown() {
    // Given:
    getActiveAndStandbyMetadata();
    when(livenessFilter.filter(eq(activeHost)))
        .thenReturn(false);
    when(livenessFilter.filter(eq(standByHost1)))
        .thenReturn(false);

    // When:
    final List<KsqlNode> result = locator.locate(SOME_KEY, routingOptions,
        routingFilterFactoryStandby);

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result.stream().findFirst().get(), is(standByNode2));
  }

  @SuppressWarnings("unchecked")
  private void getEmtpyMetadata() {
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(KeyQueryMetadata.NOT_AVAILABLE);
  }

  @SuppressWarnings("unchecked")
  private void getActiveAndStandbyMetadata() {
    when(keyQueryMetadata.activeHost()).thenReturn(activeHostInfo);
    when(keyQueryMetadata.standbyHosts()).thenReturn(ImmutableSet.of(
        standByHostInfo1, standByHostInfo2));
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(keyQueryMetadata);
  }

  @SuppressWarnings("unchecked")
  private void getActiveAndStandbyMetadata(final HostInfo activeHostInfo) {
    when(keyQueryMetadata.activeHost()).thenReturn(activeHostInfo);
    when(keyQueryMetadata.standbyHosts()).thenReturn(Collections.emptySet());
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(keyQueryMetadata);
  }

  private static URL localHost() {
    try {
      return new URL("http://somehost:1234");
    } catch (final MalformedURLException e) {
      throw new AssertionError("Failed to build URL", e);
    }
  }
}