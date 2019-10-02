/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static java.util.Objects.requireNonNull;
import static org.easymock.EasyMock.niceMock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.context.KsqlRestServiceContextBinder;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eclipse.jetty.websocket.api.util.WSURI;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.junit.rules.ExternalResource;

/**
 * Junit external resource for managing an instance of {@link KsqlRestApplication}.
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

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private final String metricsPrefix = "app-" + COUNTER.getAndIncrement() + "-";
  private final Map<String, ?> baseConfig;
  private final Supplier<String> bootstrapServers;
  private final Supplier<ServiceContext> serviceContext;
  private final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory;
  private final List<URL> listeners = new ArrayList<>();
  private final Optional<BasicCredentials> credentials;
  private KsqlRestApplication restServer;

  private TestKsqlRestApp(
      final Supplier<String> bootstrapServers,
      final Map<String, Object> additionalProps,
      final Supplier<ServiceContext> serviceContext,
      final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory,
      final Optional<BasicCredentials> credentials
  ) {
    this.baseConfig = buildBaseConfig(additionalProps);
    this.bootstrapServers = requireNonNull(bootstrapServers, "bootstrapServers");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.serviceContextBinderFactory =
        requireNonNull(serviceContextBinderFactory, "serviceContextBinderFactory");
    this.credentials = requireNonNull(credentials, "credentials");
  }

  @SuppressWarnings("WeakerAccess") // Part of public API
  public List<URL> getListeners() {
    return this.listeners;
  }

  @SuppressWarnings("unused") // Part of public API
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

  public KsqlRestClient buildKsqlClient() {
    return KsqlRestClient.create(
        getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials
    );
  }

  public KsqlRestClient buildKsqlClient(final Optional<BasicCredentials> credentials) {
    return KsqlRestClient.create(
        getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials
    );
  }

  public static String getCommandTopicName() {
    return KsqlInternalTopicUtils.getTopicName(
        new KsqlConfig(ImmutableMap.of()),
        KsqlRestConfig.COMMAND_TOPIC_SUFFIX
    );
  }

  public Set<String> getPersistentQueries() {
    try (final KsqlRestClient client = buildKsqlClient()) {
      return getPersistentQueries(client);
    }
  }

  public void closePersistentQueries() {
    try (final KsqlRestClient client = buildKsqlClient()) {
      terminateQueries(getPersistentQueries(client), client);
    }
  }

  public void dropSourcesExcept(final String... blackList) {
    try (final KsqlRestClient client = buildKsqlClient()) {

      final Set<String> except = Arrays.stream(blackList)
          .map(String::toUpperCase)
          .collect(Collectors.toSet());

      final Set<String> streams = getStreams(client);
      streams.removeAll(except);
      dropStreams(streams, client);

      final Set<String> tables = getTables(client);
      tables.removeAll(except);
      dropTables(tables, client);
    }
  }

  public static Client buildClient() {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper.copy();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
    objectMapper.registerModule(new Jdk8Module());
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    return ClientBuilder.newBuilder().register(jsonProvider).build();
  }

  public ServiceContext getServiceContext() {
    return serviceContext.get();
  }

  @Override
  protected void before() {
    if (restServer != null) {
      after();
    }

    try {
      restServer = KsqlRestApplication.buildApplication(
          metricsPrefix,
          buildConfig(bootstrapServers, baseConfig),
          (booleanSupplier) -> niceMock(VersionCheckerAgent.class),
          3,
          serviceContext.get(),
          serviceContextBinderFactory
      );
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

  private static Set<String> getPersistentQueries(final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> response = client.makeKsqlRequest("SHOW QUERIES;");
    if (response.isErroneous()) {
      throw new AssertionError("Failed to get persistent queries."
          + " msg:" + response.getErrorMessage());
    }

    final Queries queries = (Queries) response.getResponse().get(0);
    return queries.getQueries().stream()
        .map(RunningQuery::getId)
        .map(EntityQueryId::getId)
        .collect(Collectors.toSet());
  }

  private static void terminateQueries(final Set<String> queryIds, final KsqlRestClient client) {
    final HashSet<String> remaining = new HashSet<>(queryIds);
    while (!remaining.isEmpty()) {
      KsqlErrorMessage lastError = null;
      final Set<String> toRemove = new HashSet<>();

      for (final String queryId : remaining) {
        final RestResponse<KsqlEntityList> response = client
            .makeKsqlRequest("TERMINATE " + queryId + ";");

        if (response.isSuccessful()) {
          toRemove.add(queryId);
        } else {
          lastError = response.getErrorMessage();
        }
      }

      if (toRemove.isEmpty()) {
        throw new AssertionError("Failed to terminate queries. lastError:" + lastError);
      }

      remaining.removeAll(toRemove);
    }
  }

  private static Set<String> getStreams(final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res = client.makeKsqlRequest("SHOW STREAMS;");
    if (res.isErroneous()) {
      throw new AssertionError("Failed to get streams."
          + " msg:" + res.getErrorMessage());
    }

    return ((StreamsList)res.getResponse().get(0)).getStreams().stream()
        .map(SourceInfo::getName)
        .collect(Collectors.toSet());
  }

  private static Set<String> getTables(final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res = client.makeKsqlRequest("SHOW TABLES;");
    if (res.isErroneous()) {
      throw new AssertionError("Failed to get tables."
          + " msg:" + res.getErrorMessage());
    }

    return ((TablesList)res.getResponse().get(0)).getTables().stream()
        .map(SourceInfo::getName)
        .collect(Collectors.toSet());
  }

  private static void dropStreams(final Set<String> streams, final KsqlRestClient client) {
    for (final String stream : streams) {
      final RestResponse<KsqlEntityList> res = client
          .makeKsqlRequest("DROP STREAM " + stream + ";");

      if (res.isErroneous()) {
        throw new AssertionError("Failed to drop stream " + stream + "."
            + " msg:" + res.getErrorMessage());
      }
    }
  }

  private static void dropTables(final Set<String> tables, final KsqlRestClient client) {
    for (final String table : tables) {
      final RestResponse<KsqlEntityList> res = client
          .makeKsqlRequest("DROP TABLE " + table + ";");

      if (res.isErroneous()) {
        throw new AssertionError("Failed to drop table " + table + "."
            + " msg:" + res.getErrorMessage());
      }
    }
  }

  private static KsqlRestConfig buildConfig(
      final Supplier<String> bootstrapServers,
      final Map<String, ?> baseConfig) {

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

  private static ServiceContext defaultServiceContext(
      final Supplier<String> bootstrapServers,
      final Map<String, ?> baseConfig) {

    return ServiceContextFactory.create(
        new KsqlConfig(buildConfig(bootstrapServers, baseConfig).getKsqlConfigProperties()));
  }

  public static final class Builder {

    private final Supplier<String> bootstrapServers;

    private final Map<String, Object> additionalProps = new HashMap<>();

    private Supplier<ServiceContext> serviceContext;
    private BiFunction<KsqlConfig, KsqlSecurityExtension, Binder>  serviceContextBinder
        = KsqlRestServiceContextBinder::new;

    private Optional<BasicCredentials> credentials = Optional.empty();

    private Builder(final Supplier<String> bootstrapServers) {
      this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
      this.serviceContext =
          () -> defaultServiceContext(bootstrapServers, buildBaseConfig(additionalProps));
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

    public Builder withStaticServiceContext(final Supplier<ServiceContext> serviceContext) {
      this.serviceContext = serviceContext;
      this.serviceContextBinder = (config, extension) -> new AbstractBinder() {
        @Override
        protected void configure() {
          final Factory<ServiceContext> factory = new Factory<ServiceContext>() {
            @Override
            public ServiceContext provide() {
              return serviceContext.get();
            }

            @Override
            public void dispose(final ServiceContext serviceContext) {
              // do nothing because context is shared
            }
          };

          bindFactory(factory)
              .to(ServiceContext.class)
              .in(RequestScoped.class);
        }
      };

      return this;
    }

    /**
     * Set the credentials to use for any operations, e.g. listing topics etc.
     *
     * @param username the username
     * @param password the password
     * @return self
     */
    public Builder withBasicCredentials(
        final String username,
        final String password
    ) {
      this.credentials = Optional.of(BasicCredentials.of(username, password));
      return this;
    }

    public TestKsqlRestApp build() {
      return new TestKsqlRestApp(
          bootstrapServers,
          additionalProps,
          serviceContext,
          serviceContextBinder,
          credentials
      );
    }
  }
}
