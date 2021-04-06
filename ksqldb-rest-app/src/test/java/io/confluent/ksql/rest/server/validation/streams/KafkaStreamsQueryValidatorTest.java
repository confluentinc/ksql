/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.validation.streams;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamsQueryValidatorTest {
  private static final SourceName SOURCE_NAME = SourceName.of("some-source");
  private static final Map<String, Object> BASE_STREAMS_PROPERTIES = ImmutableMap.of(
      StreamsConfig.APPLICATION_ID_CONFIG, "fake-app-id",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake-bootstrap"
  );

  @Mock
  private MetaStore metaStore;
  @Mock
  private DataSource source;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private TopicDescription description;
  @Mock
  private KafkaTopicClient topicClient;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.describeTopic(any(String.class))).thenReturn(description);
    when(description.partitions()).thenReturn(
        Collections.singletonList(mock(TopicPartitionInfo.class)));
    when(metaStore.getSource(SOURCE_NAME)).thenReturn(source);
    when(source.getKafkaTopicName()).thenReturn("kafka-topic");
  }

  @Test
  public void shouldLimitBufferCacheUsage() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.of(25));
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("foo", 0, 10));
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> sandbox.onCreate(serviceContext, metaStore, givenPersistent("bar", 0, 20))
    );
  }

  @Test
  public void shouldNotThrowIfUnderCacheBufferLimit() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.of(40));
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("foo", 0, 10));
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then (no throw)
    sandbox.onCreate(serviceContext, metaStore, givenPersistent("bar", 0, 10));
  }

  @Test
  public void shouldNotCheckCacheBufferLimitIfNotSandbox() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.of(25));
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("foo", 0, 20));

    // When/Then (no throw)
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("bar", 0, 20));
  }

  @Test
  public void shouldIgnoreBufferCacheLimitIfNotSet() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.empty());
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then (no throw)
    sandbox.onCreate(serviceContext, metaStore, givenPersistent("bar", 0, 100000000000L));
  }

  @Test
  public void shouldLimitBufferCacheLimitForTransientQueries() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimitTransient(OptionalLong.of(25));
    queryValidator.onCreate(serviceContext, metaStore, givenTransient("foo", 0, 20));

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> queryValidator.onCreate(serviceContext, metaStore, givenTransient("foo", 0, 20))
    );
  }

  @Test
  public void shouldIgnoreBufferCacheLimitIfNotSetForTransientQueries() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.empty());

    // When/Then (no throw)
    queryValidator.onCreate(serviceContext, metaStore, givenTransient("foo", 0, 100000000000L));
  }

  @Test
  public void shouldCheckLimitOnStateStores() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator = validatorWithStoreLimit(OptionalInt.of(100));
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("foo", 75, 0));
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then
    assertThrows(
        KsqlException.class,
        () -> sandbox.onCreate(serviceContext, metaStore, givenPersistent("bar", 75, 0))
    );
  }

  @Test
  public void shouldIgnoreIfNoLimitSpecifiedOnStateStores() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator = validatorWithStoreLimit(OptionalInt.empty());
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then (no throw):
    sandbox.onCreate(serviceContext, metaStore, givenPersistent("bar", 1000000, 0));
  }

  @Test
  public void shouldNotCheckLimitOnStateStoresIfNotSandbox() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator = validatorWithStoreLimit(OptionalInt.of(100));
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("foo", 75, 0));

    // When/Then (no throw)
    queryValidator.onCreate(serviceContext, metaStore, givenPersistent("bar", 75, 0));
  }

  @Test
  public void shouldCleanUpStateForClosedQueries() {
    // Given:
    final KafkaStreamsQueryValidator queryValidator
        = validatorWithBufferLimit(OptionalLong.of(25));
    final QueryMetadata query = givenPersistent("foo", 0, 25);
    queryValidator.onCreate(serviceContext, metaStore, query);
    queryValidator.onDeregister(query);
    final QueryEventListener sandbox = queryValidator.createSandbox().get();

    // When/Then (no throw):
    sandbox.onCreate(serviceContext, metaStore, query);
  }

  private KafkaStreamsQueryValidator validatorWithBufferLimit(final OptionalLong globalLimit) {
    return validatorWithBufferLimit(
        KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, globalLimit);
  }

  private KafkaStreamsQueryValidator validatorWithBufferLimitTransient(
      final OptionalLong globalLimit
  ) {
    return validatorWithBufferLimit(
        KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT, globalLimit);
  }

  private KafkaStreamsQueryValidator validatorWithBufferLimit(
      final String config,
      final OptionalLong globalLimit
  ) {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(BASE_STREAMS_PROPERTIES);
    globalLimit.ifPresent(l -> builder.put(config, l));
    return new KafkaStreamsQueryValidator(new KsqlConfig(builder.build()));
  }

  private KafkaStreamsQueryValidator validatorWithStoreLimit(
      final OptionalInt globalLimit
  ) {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(BASE_STREAMS_PROPERTIES);
    globalLimit.ifPresent(l -> builder.put(KsqlConfig.KSQL_STATE_STORE_MAX, l));
    return new KafkaStreamsQueryValidator(new KsqlConfig(builder.build()));
  }

  private Map<String, Object> streamPropsWithCacheLimit(long limit) {
    return ImmutableMap.<String, Object>builder()
        .putAll(BASE_STREAMS_PROPERTIES)
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, limit)
        .build();
  }

  private PersistentQueryMetadata givenPersistent(
      final String id,
      final int nStores,
      final long cacheBuffer
  ) {
    final PersistentQueryMetadata query = mock(PersistentQueryMetadata.class);
    givenQuery(query, id, nStores, cacheBuffer);
    return query;
  }

  private TransientQueryMetadata givenTransient(
      final String id,
      final int nStores,
      final long cacheBuffer
  ) {
    final TransientQueryMetadata query = mock(TransientQueryMetadata.class);
    givenQuery(query, id, nStores, cacheBuffer);
    return query;
  }

  private void givenQuery(
      final QueryMetadata query,
      final String id,
      final int nStores,
      final long cacheBuffer
  ) {
    when(query.getQueryId()).thenReturn(new QueryId(id));
    when(query.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(cacheBuffer));
    final Topology topology = topologyWithStores(nStores);
    when(query.getTopology()).thenReturn(topology);
    when(query.getSourceNames()).thenReturn(ImmutableSet.of(SOURCE_NAME));
  }

  private Topology topologyWithStores(int nStores) {
    final Topology topology = mock(Topology.class);
    final TopologyDescription description = mock(TopologyDescription.class);
    final Subtopology subtopology = mock(Subtopology.class);
    final Processor processor = mock(Processor.class);
    when(topology.describe()).thenReturn(description);
    when(description.subtopologies()).thenReturn(ImmutableSet.of((subtopology)));
    when(subtopology.nodes()).thenReturn(ImmutableSet.of(processor));
    when(processor.stores()).thenReturn(
        IntStream.range(0, nStores).mapToObj(Integer::toString).collect(Collectors.toSet())
    );
    return topology;
  }
}