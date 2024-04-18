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
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.NetworkDisruptorClient.NetworkState;
import io.confluent.ksql.rest.server.services.InternalKsqlClientFactory;
import io.confluent.ksql.rest.server.services.TestDefaultKsqlClientFactory;
import io.confluent.ksql.rest.server.services.TestRestServiceContextFactory;
import io.confluent.ksql.rest.server.services.TestRestServiceContextFactory.InternalSimpleKsqlClientFactory;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.Identifiers;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.vertx.core.Vertx;
import io.vertx.core.net.SocketAddress;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

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

  protected static final AtomicInteger COUNTER = new AtomicInteger();
  private final TemporaryFolder temporaryFolder = KsqlTestFolder.temporaryFolder();

  protected final String metricsPrefix = "app-" + COUNTER.getAndIncrement() + "-";
  protected final Map<String, Object> baseConfig;
  protected final Supplier<String> bootstrapServers;
  protected final Supplier<ServiceContext> serviceContext;
  protected final List<URL> listeners = new ArrayList<>();
  protected final Optional<BasicCredentials> credentials;
  protected Optional<URL> internalListener;
  protected KsqlExecutionContext ksqlEngine;
  protected KsqlRestConfig ksqlRestConfig;
  protected KsqlRestApplication ksqlRestApplication;
  protected long lastCommandSequenceNumber = -1L;
  protected InternalSimpleKsqlClientFactory internalSimpleKsqlClientFactory;

  static {
    // Increase the default - it's low (100)
    System.setProperty("sun.net.maxDatagramSockets", "1024");
  }

  protected TestKsqlRestApp(
      final Supplier<String> bootstrapServers,
      final Map<String, Object> additionalProps,
      final Supplier<ServiceContext> serviceContext,
      final Optional<BasicCredentials> credentials,
      final InternalSimpleKsqlClientFactory internalSimpleKsqlClientFactory
  ) {
    this.baseConfig = buildBaseConfig(additionalProps);
    this.bootstrapServers = requireNonNull(bootstrapServers, "bootstrapServers");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.credentials = requireNonNull(credentials, "credentials");
    this.internalSimpleKsqlClientFactory = internalSimpleKsqlClientFactory;
  }

  public KsqlExecutionContext getEngine() {
    return ksqlEngine;
  }

  public List<URL> getListeners() {
    return Collections.unmodifiableList(this.listeners);
  }

  public Optional<URL> getInternalListener() {
    return this.internalListener;
  }

  @SuppressWarnings("unused") // Part of public API
  public Map<String, ?> getBaseConfig() {
    return Collections.unmodifiableMap(this.baseConfig);
  }

  @SuppressWarnings("unused") // Part of public API
  public void start() {
    this.before();
  }

  @SuppressWarnings({"unused", "WeakerAccess"}) // Part of public API
  public void stop() {
    this.after();
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getHttpListener() {
    return getListener("HTTP");
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getHttpInternalListener() {
    return getInternalListener("HTTP");
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getHttpsListener() {
    return getListener("HTTPS");
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getWsListener() {
    String suri = getHttpListener().toString().toLowerCase().replace("http:", "ws:");
    return URI.create(suri);
  }

  @SuppressWarnings("unused") // Part of public API
  public URI getWssListener() {
    String suri = getHttpsListener().toString().toLowerCase().replace("https:", "wss:");
    return URI.create(suri);
  }

  public KsqlRestClient buildKsqlClient() {
    return KsqlRestClient.create(
        getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials,
        Optional.empty()
    );
  }

  public KsqlRestClient buildKsqlClient(final Optional<BasicCredentials> credentials) {
    return KsqlRestClient.create(
        getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials,
        Optional.empty()
    );
  }

  public KsqlRestClient buildInternalKsqlClient() {
    return KsqlRestClient.create(
        getHttpInternalListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials,
        Optional.empty()
    );
  }

  public KsqlRestClient buildInternalKsqlClient(final Optional<BasicCredentials> credentials) {
    return KsqlRestClient.create(
        getHttpInternalListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        credentials,
        Optional.empty()
    );
  }

  public static String getCommandTopicName() {
    return ReservedInternalTopics.commandTopic(new KsqlConfig(ImmutableMap.of()));
  }

  public Set<String> getPersistentQueries() {
    try (final KsqlRestClient client = buildKsqlClient()) {
      return getPersistentQueries(client)
          .stream()
          .map(RunningQuery::getId)
          .map(QueryId::toString)
          .collect(Collectors.toSet());
    }
  }

  public Set<String> getTransientQueries() {
    try (final KsqlRestClient client = buildKsqlClient()) {
      return getTransientQueries(client)
          .stream()
          .map(RunningQuery::getId)
          .map(QueryId::toString)
          .collect(Collectors.toSet());
    }
  }

  public void closePersistentQueries() {
    closePersistentQueries(Optional.empty());
  }

  public void closePersistentQueries(final Optional<BasicCredentials> credentials) {
    try (final KsqlRestClient client = buildKsqlClient(credentials)) {
      // Filter source tables queries because they cannot be terminated manually
      final Set<String> queriesToTerminate = getPersistentQueries(client).stream()
          .filter(query -> !query.getQueryString().startsWith("CREATE SOURCE TABLE"))
          .map(RunningQuery::getId)
          .map(QueryId::toString)
          .collect(Collectors.toSet());

      terminateQueries(queriesToTerminate, client);
    }
  }

  public void dropSourcesExcept(final String... exceptSources) {
    dropSourcesExcept(Optional.empty(), exceptSources);
  }

  public void dropSourcesExcept(
      final Optional<BasicCredentials> credential,
      final String... exceptSources
  ) {
    try (final KsqlRestClient client = buildKsqlClient(credential)) {

      final Set<String> except = Arrays.stream(exceptSources)
          .map(String::toUpperCase)
          .collect(Collectors.toSet());

      final Set<String> streams = getStreams(client);
      streams.removeAll(except);
      final Set<String> tables = getTables(client);
      tables.removeAll(except);

      dropSources(streams, tables, client);
    }
  }

  public ServiceContext getServiceContext() {
    return serviceContext.get();
  }

  public KsqlRestConfig getKsqlRestConfig() {
    return ksqlRestConfig;
  }

  @Override
  protected void before() {
    initialize();

    try {
      ksqlRestApplication.startAsync();
      listeners.addAll(ksqlRestApplication.getListeners());
      internalListener = ksqlRestApplication.getInternalListener();
    } catch (final Exception var2) {
      throw new RuntimeException("Failed to start Ksql rest server", var2);
    }

    ksqlEngine = ksqlRestApplication.getEngine();
  }

  @Override
  protected void after() {
    if (ksqlRestApplication == null) {
      return;
    }

    listeners.clear();
    internalListener = null;
    try {
      ksqlRestApplication.shutdown();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    ksqlRestApplication = null;
    lastCommandSequenceNumber = -1;
    temporaryFolder.delete();
  }

  protected void initialize() {
    if (ksqlRestApplication != null) {
      after();
    }

    // Make sure that we set a different state directory for every run if none is set.
    if (!baseConfig.containsKey(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG) &&
        !baseConfig.containsKey(StreamsConfig.STATE_DIR_CONFIG)) {
      try {
        this.temporaryFolder.create();
      } catch (IOException e) {
        throw new RuntimeException("Cannot create temporary folder");
      }
      baseConfig.put(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG,
          temporaryFolder.getRoot().getAbsolutePath());
    }

    ksqlRestConfig = buildConfig(bootstrapServers, baseConfig);

    try {
      Vertx vertx = Vertx.vertx();
      final MetricCollectors metricsCollector = new MetricCollectors();
      final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
      ksqlRestApplication = KsqlRestApplication.buildApplication(
          metricsPrefix,
          ksqlRestConfig,
          new ServerState(),
          (booleanSupplier) -> mock(VersionCheckerAgent.class),
          3,
          serviceContext.get(),
          () -> serviceContext.get().getSchemaRegistryClient(),
          (authHeader, requestHeaders, userPrincipal) -> serviceContext.get().getConnectClient(),
          vertx,
          InternalKsqlClientFactory.createInternalClient(
              PropertiesUtil.toMapStrings(ksqlRestConfig.originals()),
              SocketAddress::inetSocketAddress,
              vertx),
          TestRestServiceContextFactory.createDefault(internalSimpleKsqlClientFactory),
          TestRestServiceContextFactory.createUser(internalSimpleKsqlClientFactory),
          metricsCollector,
          functionRegistry,
          Instant.now()
      );
      UserFunctionLoader.newInstance(
          new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties()),
          functionRegistry,
          ksqlRestConfig.getString(KsqlRestConfig.INSTALL_DIR_CONFIG),
          metricsCollector.getMetrics()
      ).load();

    } catch (final Exception e) {
      throw new RuntimeException("Failed to initialise", e);
    }
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

  private URI getInternalListener(final String protocol) {
    final URL url = getInternalListener()
        .filter(l -> l.getProtocol().equalsIgnoreCase(protocol))
        .orElseThrow(() -> new IllegalStateException("No " + protocol + " Listener found"));

    try {
      return url.toURI();
    } catch (final Exception e) {
      throw new IllegalStateException("Invalid REST listener", e);
    }
  }

  private Set<RunningQuery> getPersistentQueries(final KsqlRestClient client) {
    return getQueries(client, KsqlQueryType.PERSISTENT);
  }

  private Set<RunningQuery> getTransientQueries(final KsqlRestClient client) {
    return getQueries(client, KsqlQueryType.PUSH);
  }

  private Set<RunningQuery> getQueries(final KsqlRestClient client,
                                 final KsqlQueryType queryType) {
    final RestResponse<KsqlEntityList> response = makeKsqlRequest(client, "SHOW QUERIES;");
    if (response.isErroneous()) {
      throw new AssertionError("Failed to get persistent queries."
          + " msg:" + response.getErrorMessage());
    }

    final Queries queries = (Queries) response.getResponse().get(0);
    return queries.getQueries().stream()
        .filter(query -> query.getQueryType() == queryType)
        .collect(Collectors.toSet());
  }

  private void terminateQueries(final Set<String> queryIds, final KsqlRestClient client) {
    final HashSet<String> remaining = new HashSet<>(queryIds);
    while (!remaining.isEmpty()) {
      KsqlErrorMessage lastError = null;
      final Set<String> toRemove = new HashSet<>();

      for (final String queryId : remaining) {
        final RestResponse<KsqlEntityList> response =
            makeKsqlRequest(client, "TERMINATE " + queryId + ";");

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

  private Set<String> getStreams(final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res = makeKsqlRequest(client, "SHOW STREAMS;");
    if (res.isErroneous()) {
      throw new AssertionError("Failed to get streams."
          + " msg:" + res.getErrorMessage());
    }

    return ((StreamsList)res.getResponse().get(0)).getStreams().stream()
        .map(SourceInfo::getName)
        .collect(Collectors.toSet());
  }

  private Set<String> getTables(final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res = makeKsqlRequest(client, "SHOW TABLES;");
    if (res.isErroneous()) {
      throw new AssertionError("Failed to get tables."
          + " msg:" + res.getErrorMessage());
    }

    return ((TablesList)res.getResponse().get(0)).getTables().stream()
        .map(SourceInfo::getName)
        .collect(Collectors.toSet());
  }

  private void dropSources(
      final Set<String> streams,
      final Set<String> tables,
      final KsqlRestClient client
  ) {
    final Set<String> sourcesDropped = new HashSet<>(streams.size() + tables.size());
    Iterables.concat(streams, tables).forEach(source -> {
      if (!sourcesDropped.contains(source)) {
        final Iterator<String> dropInOrder = getOrderedSourcesToDrop(source, client);
        dropInOrder.forEachRemaining(s -> {
          if (streams.contains(s)) {
            dropStream(s, client);
          } else {
            dropTable(s, client);
          }
          sourcesDropped.add(s);
        });
      }
    });
  }

  private void dropStream(final String stream, final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res =
        makeKsqlRequest(client, "DROP STREAM `" + stream + "`;");

    if (res.isErroneous()) {
      throw new AssertionError("Failed to drop stream " + stream + "."
          + " msg:" + res.getErrorMessage());
    }
  }

  private void dropTable(final String table, final KsqlRestClient client) {
    final RestResponse<KsqlEntityList> res =
        makeKsqlRequest(client, "DROP TABLE `" + table + "`;");

    if (res.isErroneous()) {
      throw new AssertionError("Failed to drop table " + table + "."
          + " msg:" + res.getErrorMessage());
    }
  }

  /**
   * Returns an ordered list of sources to drop. Sources may have referenced sources that havea a
   * DROP constraint against this source. If that's the case, this method walks through the each
   * linked source and builds a list of sources that must be dropped in order.
   *
   * Example:
   * - CREATE STREAM 'a';
   * - CREATE STREAM 'b' AS SELECT FROM 'a';
   * - CREATE STREAM 'c' AS SELECT FROM 'b';
   *
   * In the above example. the source 'a' has a drop constraint reference from 'b', and 'b' has
   * a drop constraint reference from 'c'. Source 'a' cannot be dropped until all the child sources
   * are dropped. This method will return a list with ['c', 'b', 'a'], which tells the caller to
   * drop 'c' first, 'b' second, and 'a' third.
   */
  private Iterator<String> getOrderedSourcesToDrop(
      final String source,
      final KsqlRestClient client
  ) {
    final Set<String> visited = new LinkedHashSet<>();
    final Stack<String> stack = new Stack<>();

    stack.push(source);
    while (!stack.isEmpty()) {
      String s = stack.pop();
      if (!visited.contains(s)) {
        visited.add(s);

        final RestResponse<KsqlEntityList> res =
            makeKsqlRequest(client, "DESCRIBE `" + s + "` EXTENDED;");

        if (res.isErroneous()) {
          throw new AssertionError("Failed to describe stream " + s + "."
              + " msg:" + res.getErrorMessage());
        }

        final List<String> sourceConstraints = ((SourceDescriptionEntity)(res.getResponse().get(0)))
            .getSourceDescription().getSourceConstraints();

        sourceConstraints.forEach(name -> stack.push(name));
      }
    }

    return visited.stream().collect(Collectors.toCollection(LinkedList::new)).descendingIterator();
  }

  private RestResponse<KsqlEntityList> makeKsqlRequest(
      final KsqlRestClient client,
      final String request
  ) {
    final RestResponse<KsqlEntityList> response =
        client.makeKsqlRequest(request, lastCommandSequenceNumber);

    lastCommandSequenceNumber = response.getResponse().stream()
        .filter(entity -> entity instanceof CommandStatusEntity)
        .map(entity -> (CommandStatusEntity)entity)
        .mapToLong(CommandStatusEntity::getCommandSequenceNumber)
        .max()
        .orElse(lastCommandSequenceNumber);

    return response;
  }

  private static KsqlRestConfig buildConfig(
      final Supplier<String> bootstrapServers,
      final Map<String, ?> baseConfig) {

    final HashMap<String, Object> config = new HashMap<>(baseConfig);

    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.get());
    config.putIfAbsent(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0");
    config.put(KsqlRestConfig.VERTICLE_INSTANCES, 4);
    return new KsqlRestConfig(config);
  }

  private static Map<String, Object> buildBaseConfig(final Map<String, ?> additionalProps) {

    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "application.id", "KSQL");
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "commit.interval.ms", 0);
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "cache.max.bytes.buffering", 0);
    configMap.put(KsqlConfig.KSQL_STREAMS_PREFIX + "auto.offset.reset", "earliest");
    configMap.put(KsqlConfig.KSQL_ENABLE_UDFS, false);
    configMap.put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, false);
    configMap.put(KsqlConfig.KSQL_QUERY_CLEANUP_SHUTDOWN_TIMEOUT_MS, 500L);
    configMap.put(KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG, 15000L);
    configMap.putAll(additionalProps);
    return configMap;
  }

  private static ServiceContext defaultServiceContext(
      final Supplier<String> bootstrapServers,
      final Map<String, ?> baseConfig,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {
    final KsqlConfig config =
        new KsqlConfig(buildConfig(bootstrapServers, baseConfig).getKsqlConfigProperties());

    return ServiceContextFactory.create(config, ksqlClientSupplier);
  }

  public static final class Builder {

    private final Supplier<String> bootstrapServers;

    private final Map<String, Object> additionalProps = new HashMap<>();

    private Supplier<ServiceContext> serviceContext;

    private Optional<BasicCredentials> credentials = Optional.empty();

    private InternalSimpleKsqlClientFactory internalSimpleKsqlClientFactory;

    private Builder(final Supplier<String> bootstrapServers) {
      this.bootstrapServers = requireNonNull(bootstrapServers, "bootstrapServers");
      this.serviceContext =
          () -> defaultServiceContext(bootstrapServers, buildBaseConfig(additionalProps),
              DisabledKsqlClient::instance);
      this.internalSimpleKsqlClientFactory = TestDefaultKsqlClientFactory::instance;
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

    // Rather than having ksql client calls disabled, creates a real instance suitable for
    // functional tests.
    public Builder withEnabledKsqlClient(
        final BiFunction<Integer, String, SocketAddress> socketAddressFactory) {
      this.serviceContext =
          () -> defaultServiceContext(bootstrapServers, buildBaseConfig(additionalProps),
              () -> TestDefaultKsqlClientFactory.instance(additionalProps, socketAddressFactory));
      return this;
    }

    public Builder withEnabledKsqlClient() {
      withEnabledKsqlClient(SocketAddress::inetSocketAddress);
      return this;
    }

    public Builder withNetworkDisruptorInternalKsqlClient(NetworkState networkState) {
      internalSimpleKsqlClientFactory = (authHeader, ksqlClient) ->
          new NetworkDisruptorClient(
              TestDefaultKsqlClientFactory.instance(authHeader, ksqlClient), networkState);
      return this;
    }

    public Builder withFaultyKsqlClient(Supplier<Boolean> cutoff) {
      this.serviceContext =
          () -> defaultServiceContext(bootstrapServers, buildBaseConfig(additionalProps),
              () -> new FaultyKsqlClient(TestDefaultKsqlClientFactory.instance(additionalProps),
                  cutoff));
      return this;
    }

    public Builder withStaticServiceContext(final Supplier<ServiceContext> serviceContext) {
      this.serviceContext = serviceContext;
      return this;
    }

    /**
     * Set the credentials to use to build the client and used for any internal operations, e.g.
     * {@link #dropSourcesExcept(String...)}, {@link #closePersistentQueries} etc.
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
          credentials,
          internalSimpleKsqlClientFactory
      );
    }
  }
}
