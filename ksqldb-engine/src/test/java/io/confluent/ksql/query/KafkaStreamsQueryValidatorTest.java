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

package io.confluent.ksql.query;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.execution.ExecutionPlan;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collection;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamsQueryValidatorTest {
  private static final Map<String, Object> BASE_STREAMS_PROPERTIES = ImmutableMap.of(
      StreamsConfig.APPLICATION_ID_CONFIG, "fake-app-id",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake-bootstrap"
  );

  @Mock
  private TransientQueryMetadata transientQueryMetadata1;
  @Mock
  private TransientQueryMetadata transientQueryMetadata2;
  @Mock
  private PersistentQueryMetadata persistentQueryMetadata1;
  @Mock
  private  PersistentQueryMetadata persistentQueryMetadata2;
  @Mock
  private BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata;
  @Mock
  private BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata2;
  @Mock
  private ExecutionPlan plan;

  private Collection<QueryMetadata> queries;

  private final QueryValidator queryValidator = new KafkaStreamsQueryValidator();

  @Before
  public void setup() {
    when(transientQueryMetadata1.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(10));
    when(transientQueryMetadata2.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(20));
    when(persistentQueryMetadata1.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(10));
    when(persistentQueryMetadata2.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(20));
    when(binPackedPersistentQueryMetadata.getQueryApplicationId()).thenReturn("runtime 1");
    when(binPackedPersistentQueryMetadata2.getQueryApplicationId()).thenReturn("runtime 1");
    when(binPackedPersistentQueryMetadata.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(10));
    when(binPackedPersistentQueryMetadata2.getStreamsProperties()).thenReturn(streamPropsWithCacheLimit(10));
    queries = ImmutableList.of(
        transientQueryMetadata1,
        transientQueryMetadata2,
        persistentQueryMetadata1,
        persistentQueryMetadata2,
        binPackedPersistentQueryMetadata,
        binPackedPersistentQueryMetadata2
    );
  }

  @Test
  public void shouldLimitBufferCacheUsage() {
    // Given:
    final SessionConfig config = configWithLimits(5, OptionalLong.of(30));

    // When/Then:
    assertThrows(KsqlException.class, () -> queryValidator.validateQuery(config, plan, queries));
  }

  @Test
  public void shouldNotThrowIfUnderLimit() {
    // Given:
    final SessionConfig config = configWithLimits(5, OptionalLong.of(60));

    // When/Then (no throw)
    queryValidator.validateQuery(config, plan, queries);
  }

  @Test
  public void shouldIgnoreBufferCacheLimitIfNotSet() {
    // Given:
    final SessionConfig config = configWithLimits(100000000000L, OptionalLong.empty());

    // When/Then (no throw)
    queryValidator.validateQuery(config, plan, queries);
  }

  @Test
  public void shouldLimitBufferCacheLimitForTransientQueries() {
    // Given:
    final SessionConfig config = configWithLimitsTransient(5, OptionalLong.of(30));

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> queryValidator.validateTransientQuery(config, plan, queries)
    );
  }

  @Test
  public void shouldIgnoreBufferCacheLimitIfNotSetForTransientQueries() {
    // Given:
    final SessionConfig config = configWithLimits(100000000000L, OptionalLong.empty());

    // When/Then (no throw)
    queryValidator.validateTransientQuery(config, plan, queries);
  }

  @Test
  public void shouldIgnoreGlobalLimitSetInOverrides() {
    // Given:
    final SessionConfig config = SessionConfig.of(
        new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, 30)),
        ImmutableMap.of(
            StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 50,
            KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, 500
        )
    );

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> queryValidator.validateQuery(config, plan, queries)
    );
  }

  @Test
  public void shouldValidateSharedRuntimes() {
    // Given:
    final SessionConfig config = SessionConfig.of(
        new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, 50,
        KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)),
        ImmutableMap.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10)
    );

    // When/Then:
    queryValidator.validateQuery(config, plan, queries);
  }

  @Test
  public void shouldNotValidateSharedRuntimesWhenCreatingAnewRuntimeWouldGoOverTheLimit() {
    // Given:
    final SessionConfig config = SessionConfig.of(
        new KsqlConfig(ImmutableMap.of(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, 50,
            KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)),
        ImmutableMap.of(
            StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 50,
            KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, 500
        )
    );

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> queryValidator.validateQuery(config, plan, queries)
    );
  }

  private SessionConfig configWithLimits(
      final long queryLimit,
      final OptionalLong globalLimit
  ) {
    return configWithLimits(
        KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING, queryLimit, globalLimit);
  }

  private SessionConfig configWithLimitsTransient(
      final long queryLimit,
      final OptionalLong globalLimit
  ) {
    return configWithLimits(
        KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT, queryLimit, globalLimit);
  }

  private SessionConfig configWithLimits(
      final String config,
      final long queryLimit,
      final OptionalLong globalLimit
  ) {
    final Map<String, Object> ksqlProperties = globalLimit.isPresent()
        ? ImmutableMap.<String, Object>builder()
            .putAll(BASE_STREAMS_PROPERTIES).put(config, globalLimit.getAsLong()).build()
        : BASE_STREAMS_PROPERTIES;
    return SessionConfig.of(
        new KsqlConfig(ksqlProperties),
        ImmutableMap.of(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, queryLimit)
    );
  }

  private ImmutableMap<String, Object> streamPropsWithCacheLimit(long limit) {
    return ImmutableMap.<String, Object>builder()
        .putAll(BASE_STREAMS_PROPERTIES)
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, limit)
        .build();
  }
}