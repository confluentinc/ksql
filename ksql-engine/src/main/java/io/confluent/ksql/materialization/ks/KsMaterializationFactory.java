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

package io.confluent.ksql.materialization.ks;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.materialization.Locator;
import io.confluent.ksql.model.WindowType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

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
   * @param windowType the window type of the key.
   * @param streamsProperties the Kafka Streams properties.
   * @return the new instance if the streams props support IQ.
   */
  public Optional<KsMaterialization> create(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final Serializer<Struct> keySerializer,
      final Optional<WindowType> windowType,
      final Map<String, ?> streamsProperties
  ) {
    final Object appServer = streamsProperties.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
    if (appServer == null) {
      return Optional.empty();
    }

    final URL localHost = buildLocalHost(appServer);

    final KsLocator locator = locatorFactory.create(
        stateStoreName,
        kafkaStreams,
        keySerializer,
        localHost
    );

    final KsStateStore stateStore = storeFactory.create(
        stateStoreName,
        kafkaStreams
    );

    final KsMaterialization materialization = materializationFactory.create(
        windowType,
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
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(StreamsConfig.APPLICATION_SERVER_CONFIG + " malformed: "
          + "'" + appServer + "'");
    }
  }

  interface LocatorFactory {

    KsLocator create(
        String stateStoreName,
        KafkaStreams kafkaStreams,
        Serializer<Struct> keySerializer,
        URL localHost
    );
  }

  interface StateStoreFactory {

    KsStateStore create(
        String stateStoreName,
        KafkaStreams kafkaStreams
    );
  }

  interface MaterializationFactory {

    KsMaterialization create(
        Optional<WindowType> windowType,
        Locator locator,
        KsStateStore stateStore
    );
  }
}
