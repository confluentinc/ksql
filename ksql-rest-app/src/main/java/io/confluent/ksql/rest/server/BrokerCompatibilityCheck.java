/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.BrokerNotFoundException;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.util.KsqlException;


public class BrokerCompatibilityCheck implements Closeable {

  private static Logger log = LoggerFactory.getLogger(BrokerCompatibilityCheck.class);

  private static final ConfigDef CONFIG = StreamsConfig.configDef()
      .withClientSslSupport()
      .withClientSaslSupport();


  public static class Config extends AbstractConfig {

    static Config fromStreamsConfig(Map<String, ?> props) {
      return new Config(props);
    }

    Config(Map<?, ?> originals) {
      super(CONFIG, originals, false);
    }

  }

  private static final int MAX_INFLIGHT_REQUESTS = 100;
  private final Config config;
  private final KafkaClient kafkaClient;
  private final List<MetricsReporter> reporters;
  private final Time time;

  private BrokerCompatibilityCheck(final Config config,
                                   final KafkaClient kafkaClient,
                                   final List<MetricsReporter> reporters,
                                   final Time time) {
    this.config = config;
    this.kafkaClient = kafkaClient;
    this.reporters = reporters;
    this.time = time;
  }


  public static BrokerCompatibilityCheck create(final Config config) {
    final Time time = new SystemTime();
    final ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config);
    final LogContext logContext = new LogContext("[BrokerCompatibilityCheck] ");

    final Map<String, String> metricTags = new LinkedHashMap<>();
    final String clientId = config.getString(StreamsConfig.CLIENT_ID_CONFIG);
    metricTags.put("client-id", clientId);

    final Metadata metadata = new Metadata(config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG),
        config.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG),
        false);

    final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());

    final MetricConfig metricConfig = new MetricConfig().samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
        .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
        .tags(metricTags);
    final List<MetricsReporter> reporters = config.getConfiguredInstances(
        ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
        MetricsReporter.class);
    reporters.add(new JmxReporter("kafka.admin.client"));
    final Metrics metrics = new Metrics(metricConfig, reporters, time);

    final Selector selector = new Selector(
        config.getLong(StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
        metrics,
        time,
        "kafka-client",
        channelBuilder,
        logContext);

    final KafkaClient kafkaClient = new NetworkClient(
        selector,
        metadata,
        clientId,
        MAX_INFLIGHT_REQUESTS, // a fixed large enough value will suffice
        config.getLong(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG),
        config.getLong(StreamsConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        config.getInt(StreamsConfig.SEND_BUFFER_CONFIG),
        config.getInt(StreamsConfig.RECEIVE_BUFFER_CONFIG),
        config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG),
        time,
        true,
        new ApiVersions(),
        logContext);

    return new BrokerCompatibilityCheck(config, kafkaClient, reporters, time);
  }

  /**
   * Check if the used brokers have version 0.10.1.x or higher.
   *
   * <p>Note, for <em>pre</em> 0.10.x brokers the broker version cannot be checked and the client will hang and retry
   * until it {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.
   *
   * @throws KsqlException if brokers have version 0.10.0.x
   */
  void checkCompatibility() throws StreamsException {
    final ClientRequest clientRequest = kafkaClient.newClientRequest(
        getAnyReadyBrokerId(),
        new ApiVersionsRequest.Builder(),
        time.milliseconds(),
        true);

    final ClientResponse clientResponse = sendRequest(clientRequest);
    if (!clientResponse.hasResponse()) {
      throw new KsqlException("Received an empty response for client request when checking broker version.");
    }
    if (!(clientResponse.responseBody() instanceof ApiVersionsResponse)) {
      throw new KsqlException("Inconsistent response type for API versions request. " +
          "Expected ApiVersionsResponse but received " + clientResponse.responseBody().getClass().getName());
    }

    final ApiVersionsResponse apiVersionsResponse =  (ApiVersionsResponse) clientResponse.responseBody();

    if (apiVersionsResponse.apiVersion(ApiKeys.CREATE_TOPICS.id) == null) {
      throw new KsqlException("KSQL requires broker version 0.10.1.x or higher.");
    }

  }

  /**
   * @return the response to the request
   * @throws TimeoutException if there was no response within {@code request.timeout.ms}
   * @throws StreamsException any other fatal error
   */
  private ClientResponse sendRequest(final ClientRequest clientRequest) {
    try {
      kafkaClient.send(clientRequest, Time.SYSTEM.milliseconds());
    } catch (final RuntimeException e) {
      throw new KsqlException("Could not send request.", e);
    }

    // Poll for the response.
    final long responseTimeout = Time.SYSTEM.milliseconds() + config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
    while (Time.SYSTEM.milliseconds() < responseTimeout) {
      final List<ClientResponse> responseList;
      try {
        responseList = kafkaClient.poll(100, Time.SYSTEM.milliseconds());
      } catch (final RuntimeException e) {
        throw new KsqlException("Could not poll.", e);
      }
      if (!responseList.isEmpty()) {
        if (responseList.size() > 1) {
          throw new KsqlException("Sent one request but received multiple or no responses.");
        }
        final ClientResponse response = responseList.get(0);
        if (response.requestHeader().correlationId() == clientRequest.correlationId()) {
          return response;
        } else {
          throw new KsqlException("Inconsistent response received from the broker "
              + clientRequest.destination() + ", expected correlation id " + clientRequest.correlationId()
              + ", but received " + response.requestHeader().correlationId());
        }
      }
    }

    throw new TimeoutException("Failed to get response from broker within timeout");
  }

  private String getAnyReadyBrokerId() {
    final Metadata metadata = new Metadata(
        config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG),
        config.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG),
        false);
    final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    metadata.update(Cluster.bootstrap(addresses), Collections.emptySet(), Time.SYSTEM.milliseconds());

    final List<Node> nodes = metadata.fetch().nodes();
    return ensureOneNodeIsReady(nodes);
  }

  /**
   *
   * @param nodes List of nodes to pick from.
   * @return The first node that is ready to accept requests.
   * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
   */
  private String ensureOneNodeIsReady(final List<Node> nodes) {
    String brokerId = null;
    final long readyTimeout = Time.SYSTEM.milliseconds() + config.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
    boolean foundNode = false;
    while (!foundNode && (Time.SYSTEM.milliseconds() < readyTimeout)) {
      for (Node node: nodes) {
        if (kafkaClient.ready(node, Time.SYSTEM.milliseconds())) {
          brokerId = Integer.toString(node.id());
          foundNode = true;
          break;
        }
      }
      try {
        kafkaClient.poll(50, Time.SYSTEM.milliseconds());
      } catch (final RuntimeException e) {
        throw new KsqlException("Could not poll.", e);
      }
    }
    if (brokerId == null) {
      throw new BrokerNotFoundException("Could not find any available broker. " +
          "Check your Config setting '" + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG + "'. " +
          "This error might also occur if you try to connect to pre-0.10 brokers. " +
          "KSQL requires broker version 0.10.1.x or higher.");
    }
    return brokerId;
  }

  public void close() {
    try {
      kafkaClient.close();
    } catch (final IOException impossible) {
      // this can actually never happen, because NetworkClient doesn't throw any exception on close()
      // we log just in case
      log.error("This error indicates a bug in the code. Please report to dev@kafka.apache.org.", impossible);
    } finally {
      for (MetricsReporter metricsReporter: this.reporters) {
        metricsReporter.close();
      }
    }
  }
}
