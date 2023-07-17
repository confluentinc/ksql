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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Factory class for {@link KsMaterialization}.
 */
public final class KsMaterializationFactory {

  private final LocatorFactory locatorFactory;
  private final StateStoreFactory storeFactory;
  private final MaterializationFactory materializationFactory;

  public KsMaterializationFactory() {
    this(
        KsLocator::new,
        KsStateStore::new,
        KsMaterialization::new
    );
  }

  @VisibleForTesting
  KsMaterializationFactory(
      final LocatorFactory locatorFactory,
      final StateStoreFactory storeFactory,
      final MaterializationFactory materializationFactory
  ) {
    this.locatorFactory = requireNonNull(locatorFactory, "locatorFactory");
    this.storeFactory = requireNonNull(storeFactory, "storeFactory");
    this.materializationFactory = requireNonNull(materializationFactory, "materializationFactory");
  }

  /**
   * Create {@link KsMaterialization} instance.
   *
   * @param stateStoreName the name of the state store in the Kafka Streams instance.
   * @param kafkaStreams the Kafka Streams instance.
   * @param keySerializer the key serializer - used purely for location lookups.
   * @param windowInfo the window type of the key.
   * @param streamsProperties the Kafka Streams properties.
   * @return the new instance if the streams props support IQ.
   */
  public Optional<KsMaterialization> create(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final Topology topology,
      final LogicalSchema schema,
      final Serializer<GenericKey> keySerializer,
      final Optional<WindowInfo> windowInfo,
      final Map<String, ?> streamsProperties,
      final KsqlConfig ksqlConfig,
      final String applicationId,
      final String queryId
  ) {
    final Object appServer = streamsProperties.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
    if (appServer == null) {
      return Optional.empty();
    }

    final URL localHost = buildLocalHost(appServer);

    final KsLocator locator = locatorFactory.create(
        stateStoreName,
        kafkaStreams,
        topology,
        keySerializer,
        localHost,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED),
        queryId
    );

    final KsStateStore stateStore = storeFactory.create(
        stateStoreName,
        kafkaStreams,
        schema,
        ksqlConfig,
        queryId
    );

    final KsMaterialization materialization = materializationFactory.create(
        windowInfo,
        locator,
        stateStore
    );

    return Optional.of(materialization);
  }

  private static URL buildLocalHost(final Object appServer) {
    if (!(appServer instanceof String)) {
      throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " not String");
    }

    try {
      return new URL((String) appServer);
    } catch (final MalformedURLException e) {
      throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " malformed: "
          + "'" + appServer + "'");
    }
  }

  interface LocatorFactory {

    KsLocator create(
        String stateStoreName,
        KafkaStreams kafkaStreams,
        Topology topology,
        Serializer<GenericKey> keySerializer,
        URL localHost,
        boolean sharedRuntimesEnabled,
        String queryId
    );
  }

  interface StateStoreFactory {

    KsStateStore create(
        String stateStoreName,
        KafkaStreams kafkaStreams,
        LogicalSchema schema,
        KsqlConfig ksqlConfig,
        String queryId
    );
  }

  interface MaterializationFactory {

    KsMaterialization create(
        Optional<WindowInfo> windowInfo,
        Locator locator,
        KsStateStore stateStore
    );
  }
}
