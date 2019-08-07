/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.connect;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ConnectConfigService} listens to the connect configuration topic,
 * which outputs messages whenever a new connector (or connector task) is started
 * in Connect. These messages contain information that is then passed to a
 * {@link ConnectPollingService} to digest and register with KSQL.
 *
 * <p>On startup, this service reads the connect configuration topic from the
 * beginning to make sure that it reconstructs the necessary state.</p>
 */
class ConnectConfigService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectConfigService.class);
  private static final long POLL_TIMEOUT_S = 60;

  private final KsqlConfig ksqlConfig;
  private final String configsTopic;
  private final ConnectPollingService pollingService;
  private final JsonConverter converter;
  private final Function<Map<String, Object>, KafkaConsumer<String, byte[]>> consumerFactory;
  private final Function<Map<String, String>, Optional<Connector>> connectorFactory;

  private Set<Connector> connectors = new HashSet<>();

  // not final because constructing a consumer is expensive and should be
  // done in startUp()
  private KafkaConsumer<String, byte[]> consumer;

  ConnectConfigService(
      final KsqlConfig ksqlConfig,
      final ConnectPollingService pollingService
  ) {
    this(ksqlConfig, pollingService, Connectors::fromConnectConfig , KafkaConsumer::new);
  }

  @VisibleForTesting
  ConnectConfigService(
      final KsqlConfig ksqlConfig,
      final ConnectPollingService pollingService,
      final Function<Map<String, String>, Optional<Connector>> connectorFactory,
      final Function<Map<String, Object>, KafkaConsumer<String, byte[]>> consumerFactory
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.pollingService = Objects.requireNonNull(pollingService, "pollingService");
    this.connectorFactory = Objects.requireNonNull(connectorFactory, "connectorFactory");
    this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
    this.configsTopic = ksqlConfig.getString(KsqlConfig.CONNECT_CONFIGS_TOPIC_PROPERTY);

    this.converter = new JsonConverter();

    addListener(new Listener() {
      @Override
      public void failed(final State from, final Throwable failure) {
        LOG.error("ConnectConfigService failed due to: ", failure);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  protected void startUp() {
    final Map<String, Object> consumerConfigs = ImmutableMap.<String, Object>builder()
        .putAll(ksqlConfig.getProducerClientConfigProps())
        // don't create the config topic if it doesn't exist - this is also necessary
        // for some integration tests to pass
        .put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
        .build();

    consumer = consumerFactory.apply(consumerConfigs);
    consumer.assign(ImmutableList.of(new TopicPartition(configsTopic, 0)));
    consumer.seekToBeginning(ImmutableList.of());

    converter.configure(ImmutableMap.of(
        ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName(),
        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false));
  }

  @Override
  protected void run() {
    while (isRunning()) {
      final ConsumerRecords<String, byte[]> records;
      try {
        records = consumer.poll(Duration.ofSeconds(POLL_TIMEOUT_S));
        LOG.debug("Polled {} records from connect config topic", records.count());
      } catch (final WakeupException e) {
        if (isRunning()) {
          throw e;
        }
        return;
      }

      final Set<Connector> connectors = new HashSet<>();
      for (final ConsumerRecord<String, byte[]> record : records) {
        try {
          extractConnector(
              converter.toConnectData(configsTopic, record.value()).value()
          ).ifPresent(connectors::add);
        } catch (final DataException e) {
          LOG.warn("Failed to read connector configuration for connector {}", record.key(), e);
        }
      }

      if (!connectors.isEmpty()) {
        connectors.forEach(pollingService::addConnector);
        if (!Sets.symmetricDifference(this.connectors, connectors).isEmpty()) {
          LOG.info("Registered the following connectors: {}", connectors);
          this.connectors = connectors;
          pollingService.runOneIteration();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<Connector> extractConnector(final Object value) {
    if (value != null) {
      final Map<String, Object> asMap = (Map<String, Object>) value;
      final Map<String, String> properties = (Map<String, String>) asMap.get("properties");
      if (properties != null) {
        return connectorFactory.apply(properties);
      }
    }

    return Optional.empty();
  }

  @Override
  protected void shutDown() {
    // this is called in the same thread as run() as it is not thread-safe
    consumer.close();
    LOG.info("ConnectConfigService is down.");
  }

  @Override
  protected void triggerShutdown() {
    // this is called on a different thread from run() since it is thread-safe
    LOG.info("Shutting down ConnectConfigService.");
    consumer.wakeup();
  }
}
