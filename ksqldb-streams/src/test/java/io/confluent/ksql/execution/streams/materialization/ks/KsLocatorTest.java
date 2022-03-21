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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsLocatorTest {

  private static final String APPLICATION_ID = "app_id";
  private static final String STORE_NAME = "someStoreName";
  private static final URL LOCAL_HOST_URL = localHost();
  private static final Schema SCHEMA = SchemaBuilder.struct().field("a", SchemaBuilder.int32());
  private static final GenericKey SOME_KEY = GenericKey.genericKey(1);
  private static final GenericKey SOME_KEY1 = GenericKey.genericKey(2);
  private static final GenericKey SOME_KEY2 = GenericKey.genericKey(3);
  private static final GenericKey SOME_KEY3 = GenericKey.genericKey(4);
  private static final KsqlKey KEY = new TestKey(SOME_KEY);
  private static final KsqlKey KEY1 = new TestKey(SOME_KEY1);
  private static final KsqlKey KEY2 = new TestKey(SOME_KEY2);
  private static final KsqlKey KEY3 = new TestKey(SOME_KEY3);
  private static final KsqlHostInfo ACTIVE_HOST = new KsqlHostInfo("remoteHost", 2345);
  private static final KsqlHostInfo STANDBY_HOST1 = new KsqlHostInfo("standby1", 1234);
  private static final KsqlHostInfo STANDBY_HOST2 = new KsqlHostInfo("standby2", 5678);
  private static final HostInfo ACTIVE_HOST_INFO = new HostInfo("remoteHost", 2345);
  private static final HostInfo STANDBY_HOST_INFO1 = new HostInfo("standby1", 1234);
  private static final HostInfo STANDBY_HOST_INFO2 = new HostInfo("standby2", 5678);
  private static final String TOPIC_NAME = "foo";
  private static final String FULL_TOPIC_NAME = APPLICATION_ID + "-" + TOPIC_NAME;
  private static final String BAD_TOPIC_NAME = "bar";
  private static final TopicPartition TOPIC_PARTITION1 = new TopicPartition(FULL_TOPIC_NAME, 0);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(FULL_TOPIC_NAME, 1);
  private static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(FULL_TOPIC_NAME, 2);
  private static final TopicPartition BAD_TOPIC_PARTITION1 = new TopicPartition(BAD_TOPIC_NAME, 0);
  private static final TopicPartition BAD_TOPIC_PARTITION2 = new TopicPartition(BAD_TOPIC_NAME, 1);
  private static final TopicPartition BAD_TOPIC_PARTITION3 = new TopicPartition(BAD_TOPIC_NAME, 2);

  private static final StreamsMetadata HOST1_STREAMS_MD1 = mock(StreamsMetadata.class);
  private static final StreamsMetadata HOST1_STREAMS_MD2 = mock(StreamsMetadata.class);
  private static final StreamsMetadata HOST1_STREAMS_MD3 = mock(StreamsMetadata.class);
  {
    when(HOST1_STREAMS_MD1.hostInfo()).thenReturn(ACTIVE_HOST_INFO);
    when(HOST1_STREAMS_MD1.stateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD1.standbyStateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD1.topicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION1, BAD_TOPIC_PARTITION3));
    when(HOST1_STREAMS_MD1.standbyTopicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION2, TOPIC_PARTITION3,
                                                                                BAD_TOPIC_PARTITION1, BAD_TOPIC_PARTITION2));

    when(HOST1_STREAMS_MD2.hostInfo()).thenReturn(STANDBY_HOST_INFO1);
    when(HOST1_STREAMS_MD2.stateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD2.standbyStateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD2.topicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION2, BAD_TOPIC_PARTITION1));
    when(HOST1_STREAMS_MD2.standbyTopicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION1, TOPIC_PARTITION3,
                                                                                BAD_TOPIC_PARTITION2, BAD_TOPIC_PARTITION3));

    when(HOST1_STREAMS_MD3.hostInfo()).thenReturn(STANDBY_HOST_INFO2);
    when(HOST1_STREAMS_MD3.stateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD3.standbyStateStoreNames()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(HOST1_STREAMS_MD3.topicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION3, BAD_TOPIC_PARTITION2));
    when(HOST1_STREAMS_MD3.standbyTopicPartitions()).thenReturn(ImmutableSet.of(TOPIC_PARTITION1, TOPIC_PARTITION2,
                                                                                BAD_TOPIC_PARTITION1, BAD_TOPIC_PARTITION3));
  }

  @Mock
  private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private Topology topology;
  @Mock
  private KeyQueryMetadata keyQueryMetadata;
  @Mock
  private Serializer<GenericKey> keySerializer;
  @Mock
  private RoutingFilter livenessFilter;
  @Mock
  private RoutingFilter activeFilter;
  @Mock
  private RoutingOptions routingOptions;
  @Mock
  private TopologyDescription description;
  @Mock
  private Subtopology sub1;
  @Mock
  private TopologyDescription.Source source;
  @Mock
  private TopologyDescription.Processor processor;

  private KsLocator locator;
  private KsqlNode activeNode;
  private KsqlNode standByNode1;
  private KsqlNode standByNode2;
  private RoutingFilters routingStandbyFilters;
  private RoutingFilters routingActiveFilters;
  private RoutingFilterFactory routingFilterFactoryActive;
  private RoutingFilterFactory routingFilterFactoryStandby;

  @Before
  public void setUp() {
    locator = new KsLocator(
        STORE_NAME,
        kafkaStreams,
        topology,
        keySerializer,
        LOCAL_HOST_URL,
        APPLICATION_ID,
        false,
        "queryId"
    );

    activeNode = locator.asNode(Host.include(ACTIVE_HOST));
    standByNode1 = locator.asNode(Host.include(STANDBY_HOST1));
    standByNode2 = locator.asNode(Host.include(STANDBY_HOST2));

    routingStandbyFilters = new RoutingFilters(ImmutableList.of(livenessFilter));
    routingActiveFilters = new RoutingFilters(ImmutableList.of(activeFilter, livenessFilter));

    // Only active serves query
    when(activeFilter.filter(eq(ACTIVE_HOST)))
        .thenReturn(Host.include(ACTIVE_HOST));
    when(activeFilter.filter(eq(STANDBY_HOST1)))
        .thenReturn(Host.exclude(STANDBY_HOST1, "active"));
    when(activeFilter.filter(eq(STANDBY_HOST2)))
        .thenReturn(Host.exclude(STANDBY_HOST2, "active"));

    // Heartbeat not enabled, all hosts alive
    when(livenessFilter.filter(eq(ACTIVE_HOST)))
        .thenReturn(Host.include(ACTIVE_HOST));
    when(livenessFilter.filter(eq(STANDBY_HOST1)))
        .thenReturn(Host.include(STANDBY_HOST1));
    when(livenessFilter.filter(eq(STANDBY_HOST2)))
        .thenReturn(Host.include(STANDBY_HOST2));

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
        () -> locator.locate(ImmutableList.of(KEY), routingOptions, routingFilterFactoryActive, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Materialized data for key [1] is not available yet. Please try again later."));
  }

  @Test
  public void shouldThrowIfRangeScanAndKeysEmpty() {
    // Given:
    getEmtpyMetadata();

    // When:
    final Exception e = assertThrows(
      IllegalStateException.class,
      () -> locator.locate(Collections.emptyList(), routingOptions, routingFilterFactoryActive, true)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Query is range scan but found no range keys."));
  }

  @Test
  public void shouldReturnOwnerIfKnown() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    final Optional<URI> url = nodeList.stream().findFirst().map(KsqlNode::location);
    assertThat(url.map(URI::getScheme), is(Optional.of(LOCAL_HOST_URL.getProtocol())));
    assertThat(url.map(URI::getHost), is(Optional.of(ACTIVE_HOST.host())));
    assertThat(url.map(URI::getPort), is(Optional.of(ACTIVE_HOST.port())));
    assertThat(url.map(URI::getPath), is(Optional.of("/")));
  }

  @Test
  public void shouldUseNamedTopologyWhenSharedRuntimeIsEnabledForStreamsMetadataForStore() {
    // Given:
    final KsLocator locator = new KsLocator(STORE_NAME, kafkaStreamsNamedTopologyWrapper, topology,
        keySerializer, LOCAL_HOST_URL, APPLICATION_ID, true, "queryId");

    // When:
    locator.getStreamsMetadata();

    // Then:
    Mockito.verify(kafkaStreamsNamedTopologyWrapper).streamsMetadataForStore(STORE_NAME, "queryId");
  }

  @Test
  public void shouldUseNamedTopologyWhenSharedRuntimeIsEnabledForQueryMetadataForKey() {
    // Given:
    final KsLocator locator = new KsLocator(STORE_NAME, kafkaStreamsNamedTopologyWrapper, topology,
        keySerializer, LOCAL_HOST_URL, APPLICATION_ID, true, "queryId");

    // When:
    locator.getKeyQueryMetadata(KEY);

    // Then:
    Mockito.verify(kafkaStreamsNamedTopologyWrapper)
        .queryMetadataForKey(STORE_NAME, KEY.getKey(), keySerializer, "queryId");
  }

  @Test
  public void shouldReturnLocalOwnerIfSameAsSuppliedLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo(LOCAL_HOST_URL.getHost(), LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY),
        routingOptions, routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnLocalOwnerIfExplicitlyLocalHostOnSamePortAsSuppliedLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("LocalHOST", LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(true)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("different", LOCAL_HOST_URL.getPort());
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.stream().findFirst().map(KsqlNode::isLocal), is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPort() {
    // Given:
    final HostInfo localHostInfo = new HostInfo(LOCAL_HOST_URL.getHost(), LOCAL_HOST_URL.getPort() + 1);
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.stream().findFirst().map(KsqlNode::isLocal),
        is(Optional.of(false)));
  }

  @Test
  public void shouldReturnRemoteOwnerForDifferentPortOnLocalHost() {
    // Given:
    final HostInfo localHostInfo = new HostInfo("LOCALhost", LOCAL_HOST_URL.getPort() + 1);
    final KsqlHostInfo localHost = locator.asKsqlHost(localHostInfo);
    getActiveAndStandbyMetadata(localHostInfo);
    when(activeFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));
    when(livenessFilter.filter(eq(localHost)))
        .thenReturn(Host.include(localHost));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.stream().findFirst().map(KsqlNode::isLocal),
        is(Optional.of(false)));
  }

  @Test
  public void shouldReturnActiveWhenRoutingStandbyNotEnabledHeartBeatNotEnabled() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryActive, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes().stream()
        .filter(node -> node.getHost().isSelected())
        .collect(Collectors.toList());
    assertThat(nodeList.size(), is(1));
    assertThat(nodeList.stream().findFirst().get(), is(activeNode));
  }

  @Test
  public void shouldReturnActiveAndStandBysWhenRoutingStandbyEnabledHeartBeatNotEnabled() {
    // Given:
    getActiveAndStandbyMetadata();

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryStandby, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.stream().findFirst().get(), is(activeNode));
    assertThat(nodeList, containsInAnyOrder(activeNode, standByNode1, standByNode2));
  }

  @Test
  public void shouldReturnStandBysWhenActiveDown() {
    // Given:
    getActiveAndStandbyMetadata();
    when(livenessFilter.filter(eq(ACTIVE_HOST)))
        .thenReturn(Host.exclude(ACTIVE_HOST, "liveness"));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryStandby, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes().stream()
        .filter(node -> node.getHost().isSelected())
        .collect(Collectors.toList());;
    assertThat(nodeList.size(), is(2));
    assertThat(nodeList, containsInAnyOrder(standByNode1, standByNode2));
  }

  @Test
  public void shouldReturnOneStandByWhenActiveAndOtherStandByDown() {
    // Given:
    getActiveAndStandbyMetadata();
    when(livenessFilter.filter(eq(ACTIVE_HOST)))
        .thenReturn(Host.exclude(ACTIVE_HOST, "liveness"));
    when(livenessFilter.filter(eq(STANDBY_HOST1)))
        .thenReturn(Host.exclude(STANDBY_HOST1, "liveness"));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(ImmutableList.of(KEY), routingOptions,
        routingFilterFactoryStandby, false);

    // Then:
    List<KsqlNode> nodeList = result.get(0).getNodes().stream()
        .filter(node -> node.getHost().isSelected())
        .collect(Collectors.toList());
    assertThat(nodeList.size(), is(1));
    assertThat(nodeList.stream().findFirst().get(), is(standByNode2));
  }

  @Ignore
  @Test
  //For issue #7174. Temporarily ignore this test. It will call getMetadataForAllPartitions().
  //Formerly it called getMetadataForKeys().
  public void shouldGroupKeysByLocation() {
    // Given:
    getActiveStandbyMetadata(SOME_KEY, 0, ACTIVE_HOST_INFO, STANDBY_HOST_INFO1);
    getActiveStandbyMetadata(SOME_KEY1, 1, STANDBY_HOST_INFO1, ACTIVE_HOST_INFO);
    getActiveStandbyMetadata(SOME_KEY2, 0, ACTIVE_HOST_INFO, STANDBY_HOST_INFO1);
    getActiveStandbyMetadata(SOME_KEY3, 2, ACTIVE_HOST_INFO, STANDBY_HOST_INFO1);

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(
        ImmutableList.of(KEY, KEY1, KEY2, KEY3), routingOptions,
        routingFilterFactoryStandby, false);

    // Then:
    assertThat(result.size(), is(3));
    assertThat(result.get(0).getKeys().get(), contains(KEY, KEY2));
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.size(), is(2));
    assertThat(nodeList.get(0), is(activeNode));
    assertThat(nodeList.get(1), is(standByNode1));
    assertThat(result.get(1).getKeys().get(), contains(KEY1));
    nodeList = result.get(1).getNodes();
    assertThat(nodeList.size(), is(2));
    assertThat(nodeList.get(0), is(standByNode1));
    assertThat(nodeList.get(1), is(activeNode));
    assertThat(result.get(2).getKeys().get(), contains(KEY3));
    nodeList = result.get(2).getNodes();
    assertThat(nodeList.size(), is(2));
    assertThat(nodeList.get(0), is(activeNode));
    assertThat(nodeList.get(1), is(standByNode1));
  }

  @Test
  public void shouldFindAllPartitionsWhenNoKeys() {
    // Given:
    when(topology.describe()).thenReturn(description);
    when(description.subtopologies()).thenReturn(ImmutableSet.of(sub1));
    when(sub1.nodes()).thenReturn(ImmutableSet.of(source, processor));
    when(source.topicSet()).thenReturn(ImmutableSet.of(TOPIC_NAME));
    when(processor.stores()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(kafkaStreams.streamsMetadataForStore(any()))
        .thenReturn(ImmutableList.of(HOST1_STREAMS_MD1, HOST1_STREAMS_MD2, HOST1_STREAMS_MD3));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(
        ImmutableList.of(), routingOptions, routingFilterFactoryStandby, false);

    // Then:
    assertThat(result.size(), is(3));
    int partition = result.get(0).getPartition();
    assertThat(partition, is(0));
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(activeNode));
    assertThat(nodeList.get(1), is(standByNode1));
    assertThat(nodeList.get(2), is(standByNode2));
    partition = result.get(1).getPartition();
    assertThat(partition, is(1));
    nodeList = result.get(1).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(standByNode1));
    assertThat(nodeList.get(1), is(activeNode));
    assertThat(nodeList.get(2), is(standByNode2));
    partition = result.get(2).getPartition();
    assertThat(partition, is(2));
    nodeList = result.get(2).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(standByNode2));
    assertThat(nodeList.get(1), is(activeNode));
    assertThat(nodeList.get(2), is(standByNode1));
  }

  @Test
  public void shouldFindAllPartitionsWithKeysAndRangeScan() {
    // Given:
    when(topology.describe()).thenReturn(description);
    when(description.subtopologies()).thenReturn(ImmutableSet.of(sub1));
    when(sub1.nodes()).thenReturn(ImmutableSet.of(source, processor));
    when(source.topicSet()).thenReturn(ImmutableSet.of(TOPIC_NAME));
    when(processor.stores()).thenReturn(ImmutableSet.of(STORE_NAME));
    when(kafkaStreams.streamsMetadataForStore(any()))
      .thenReturn(ImmutableList.of(HOST1_STREAMS_MD1, HOST1_STREAMS_MD2, HOST1_STREAMS_MD3));

    // When:
    final List<KsqlPartitionLocation> result = locator.locate(
      ImmutableList.of(KEY), routingOptions, routingFilterFactoryStandby, true);

    // Then:
    assertThat(result.size(), is(3));
    int partition = result.get(0).getPartition();
    assertThat(partition, is(0));
    List<KsqlNode> nodeList = result.get(0).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(activeNode));
    assertThat(nodeList.get(1), is(standByNode1));
    assertThat(nodeList.get(2), is(standByNode2));
    partition = result.get(1).getPartition();
    assertThat(partition, is(1));
    nodeList = result.get(1).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(standByNode1));
    assertThat(nodeList.get(1), is(activeNode));
    assertThat(nodeList.get(2), is(standByNode2));
    partition = result.get(2).getPartition();
    assertThat(partition, is(2));
    nodeList = result.get(2).getNodes();
    assertThat(nodeList.size(), is(3));
    assertThat(nodeList.get(0), is(standByNode2));
    assertThat(nodeList.get(1), is(activeNode));
    assertThat(nodeList.get(2), is(standByNode1));
  }

  @SuppressWarnings("unchecked")
  private void getEmtpyMetadata() {
    when(kafkaStreams.queryMetadataForKey(any(), any(), any(Serializer.class)))
        .thenReturn(KeyQueryMetadata.NOT_AVAILABLE);
  }

  @SuppressWarnings("unchecked")
  private void getActiveAndStandbyMetadata() {
    when(keyQueryMetadata.activeHost()).thenReturn(ACTIVE_HOST_INFO);
    when(keyQueryMetadata.standbyHosts()).thenReturn(ImmutableSet.of(
        STANDBY_HOST_INFO1, STANDBY_HOST_INFO2));
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

  @SuppressWarnings("unchecked")
  private void getActiveStandbyMetadata(final GenericKey key, int partition,
      final HostInfo activeHostInfo, final HostInfo standByHostInfo) {
    KeyQueryMetadata keyQueryMetadata = mock(KeyQueryMetadata.class);
    when(keyQueryMetadata.activeHost()).thenReturn(activeHostInfo);
    when(keyQueryMetadata.standbyHosts()).thenReturn(ImmutableSet.of(standByHostInfo));
    when(keyQueryMetadata.partition()).thenReturn(partition);
    when(kafkaStreams.queryMetadataForKey(any(), eq(key), any(Serializer.class)))
        .thenReturn(keyQueryMetadata);
  }

  private static URL localHost() {
    try {
      return new URL("http://somehost:1234");
    } catch (final MalformedURLException e) {
      throw new AssertionError("Failed to build URL", e);
    }
  }

  private static class TestKey implements KsqlKey {

    private final GenericKey genericKey;

    public TestKey(final GenericKey genericKey) {
      this.genericKey = genericKey;
    }

    @Override
    public GenericKey getKey() {
      return genericKey;
    }

    public String toString() {
      return genericKey.toString();
    }
  }
}