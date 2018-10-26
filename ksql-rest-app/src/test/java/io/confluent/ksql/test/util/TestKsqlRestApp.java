/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.test.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.easymock.EasyMock;
import org.eclipse.jetty.websocket.api.util.WSURI;
import org.junit.rules.ExternalResource;

/**
 * Junit external resource for managing an instance of
 * {@link io.confluent.ksql.rest.server.KsqlRestApplication}.
 *
 * Generally used in conjunction with {@link EmbeddedSingleNodeKafkaCluster}
 *
 * <pre>{@code
 *   private static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
 *
 *   private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
 *       .builder(CLUSTER::bootstrapServers)
 *       .build();
 *
 *   @ClassRule
 *   public static final RuleChain CHAIN = RuleChain.outerRule(CLUSTER).around(REST_APP);
 * }</pre>
 */
public class TestKsqlRestApp extends ExternalResource {

  private final Map<String, ?> baseConfig;
  private final Supplier<String> bootstrapServers;
  private final List<URL> listeners = new ArrayList<>();
  private KsqlRestApplication restServer;

  private TestKsqlRestApp(
      final Supplier<String> bootstrapServers,
      final Map<String, Object> additionalProps) {

    this.baseConfig = buildBaseConfig(additionalProps);
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
  }

  public List<URL> getListeners() {
    return this.listeners;
  }

  public Map<String, ?> getBaseConfig() {
    return Collections.unmodifiableMap(this.baseConfig);
  }

  @SuppressWarnings("unused") // Part of public API
  public void start() {
    this.before();
  }

  @SuppressWarnings("unused") // Part of public API
  public void stop() {
    this.after();
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getHttpListener() {
    return getListener("HTTP");
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getHttpsListener() {
    return getListener("HTTPS");
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getWsListener() {
    try {
      return WSURI.toWebsocket(getHttpListener());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid WS listener", e);
    }
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getWssListener() {
    try {
      return WSURI.toWebsocket(getHttpsListener());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid WS listener", e);
    }
  }

  @SuppressWarnings("unused") // Part of public API
  public static Client buildClient() {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
    objectMapper.registerModule(new Jdk8Module());
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    return ClientBuilder.newBuilder().register(jsonProvider).build();
  }

  @Override
  protected void before() {
    if (restServer != null) {
      after();
    }

    try {
      restServer = KsqlRestApplication.buildApplication(buildConfig());
    } catch (final Exception e) {
      throw new RuntimeException("Failed to initialise", e);
    }

    try {
      restServer.start();
      listeners.addAll(restServer.getListeners());
    } catch (Exception var2) {
      throw new RuntimeException("Failed to start Ksql rest server", var2);
    }
  }

  @Override
  protected void after() {
    if (restServer == null) {
      return;
    }

    listeners.clear();
    restServer.stop();
    restServer = null;
  }

  public static Builder builder(final Supplier<String> bootstrapServers) {
    return new Builder(bootstrapServers);
  }

  private URI getListener(final String protocol) {
    final URL url = getListeners().stream()
        .filter(l -> l.getProtocol().equalsIgnoreCase(protocol))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No " + protocol + " Listener found"));

    try {
      return url.toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Invalid REST listener", e);
    }
  }

  private KsqlRestConfig buildConfig() {
    final HashMap<String, Object> config = new HashMap<>(baseConfig);

    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.get());
    config.putIfAbsent(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0,https://localhost:0");
    return new KsqlRestConfig(config);
  }

  private static Map<String, ?> buildBaseConfig(final Map<String, ?> additionalProps) {

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "application.id", "KSQL");
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "commit.interval.ms", 0);
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "cache.max.bytes.buffering", 0);
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "auto.offset.reset", "earliest");
    configMap.put(KsqlConfig.KSQL_ENABLE_UDFS, false);

    configMap.putAll(additionalProps);
    return configMap;
  }

  public static final class Builder {

    private final Supplier<String> bootstrapServers;

    private final Map<String, Object> additionalProps = new HashMap<>();

    private Builder(final Supplier<String> bootstrapServers) {
      this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    }

    @SuppressWarnings("unused") // Part of public API
    public Builder withProperty(final String name, final Object value) {
      additionalProps.put(name, value);
      return this;
    }

    @SuppressWarnings("unused") // Part of public API
    public Builder withProperties(final Map<String, ?> props) {
      additionalProps.putAll(props);
      return this;
    }

    public TestKsqlRestApp build() {
      return new TestKsqlRestApp(bootstrapServers, additionalProps);
    }
  }

  private static class NoOpVersionCheckerAgent implements VersionCheckerAgent {

    @Override
    public void start(final KsqlModuleType moduleType, final Properties ksqlProperties) {
    }
  }
}
