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
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ConnectClient.ConnectResponse;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
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
final class ConnectConfigService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectConfigService.class);
  private static final long POLL_TIMEOUT_S = 60;

  private final KsqlConfig ksqlConfig;
  private final String configsTopic;
  private final ConnectClient connectClient;
  private final ConnectPollingService pollingService;
  private final Function<Map<String, Object>, KafkaConsumer<String, byte[]>> consumerFactory;
  private final Function<ConnectorInfo, Optional<Connector>> connectorFactory;

  private Set<String> handledConnectors = new HashSet<>();

  // not final because constructing a consumer is expensive and should be
  // done in startUp()
  private KafkaConsumer<String, byte[]> consumer;

  ConnectConfigService(
      final KsqlConfig ksqlConfig,
      final ConnectClient connectClient,
      final ConnectPollingService pollingService
  ) {
    this(
        ksqlConfig,
        connectClient,
        pollingService,
        Connectors::fromConnectInfo,
        KafkaConsumer::new
    );
  }

  @VisibleForTesting
  ConnectConfigService(
      final KsqlConfig ksqlConfig,
      final ConnectClient connectClient,
      final ConnectPollingService pollingService,
      final Function<ConnectorInfo, Optional<Connector>> connectorFactory,
      final Function<Map<String, Object>, KafkaConsumer<String, byte[]>> consumerFactory
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.pollingService = Objects.requireNonNull(pollingService, "pollingService");
    this.connectorFactory = Objects.requireNonNull(connectorFactory, "connectorFactory");
    this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
    this.connectClient = Objects.requireNonNull(connectClient, "connectClient");
    this.configsTopic = ksqlConfig.getString(KsqlConfig.CONNECT_CONFIGS_TOPIC_PROPERTY);

    addListener(new Listener() {
      @Override
      public void failed(final State from, final Throwable failure) {
        LOG.error("ConnectConfigService failed due to: ", failure);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  protected void startUp() {
    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final Map<String, Object> consumerConfigs = ImmutableMap.<String, Object>builder()
        .putAll(ksqlConfig.getProducerClientConfigProps())
        // don't create the config topic if it doesn't exist - this is also necessary
        // for some integration tests to pass
        .put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false)
        // set the group id to be the same as the service id to make sure that only one
        // KSQL server will subscribe to the topic and handle connectors (use this as
        // a form of poor man's leader election)
        .put(ConsumerConfig.GROUP_ID_CONFIG, serviceId)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
        .build();

    consumer = consumerFactory.apply(consumerConfigs);
    consumer.subscribe(ImmutableList.of(configsTopic), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) { }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        // if we were assigned the (only) connect-configs partition, we should rebuild
        // our entire state - this ensures that only a single server in the KSQL cluster
        // handles new connectors
        checkConnectors();
      }
    });
  }

  @Override
  protected void run() {
    while (isRunning()) {
      try {
        final ConsumerRecords<String, byte[]> records;
        records = consumer.poll(Duration.ofSeconds(POLL_TIMEOUT_S));
        LOG.debug("Polled {} records from connect config topic", records.count());

        if (!records.isEmpty()) {
          checkConnectors();
        }
      } catch (final WakeupException e) {
        if (isRunning()) {
          throw e;
        }
      }
    }
  }

  private void checkConnectors() {
    try {
      // something changed in the connect configuration topic - poll connect
      // and see if we need to update our connectors
      final ConnectResponse<List<String>> allConnectors = connectClient.connectors();
      if (allConnectors.datum().isPresent()) {
        final List<String> toProcess = allConnectors.datum()
            .get()
            .stream()
            .filter(connectorName -> !handledConnectors.contains(connectorName))
            .collect(Collectors.toList());
        LOG.info("Was made aware of the following connectors: {}", toProcess);

        toProcess.forEach(this::handleConnector);
      }
    } catch (final KsqlServerException e) {
      LOG.warn("Failed to check the connectors due to some server error.", e);
    }
  }

  private void handleConnector(final String name) {
    try {
      final ConnectResponse<ConnectorInfo> describe = connectClient.describe(name);
      if (!describe.datum().isPresent()) {
        return;
      }

      handledConnectors.add(name);
      final Optional<Connector> connector = connectorFactory.apply(describe.datum().get());
      if (connector.isPresent()) {
        LOG.info("Registering connector {}", connector);
        pollingService.addConnector(connector.get());
      } else {
        LOG.warn("Ignoring unsupported connector {} ({})",
            name, describe.datum().get().config().get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
      }
    } catch (final KsqlServerException e) {
      LOG.warn("Failed to describe connector {} due to some server error.", name, e);
    }
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
