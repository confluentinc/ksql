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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.util.KsqlConstants;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ConnectPollingService} will maintain a list of {@link Connector}s
 * and for each one it will poll kafka and schema registry to see if any topics
 * created by that connector are eligible for automatic registry with KSQL on
 * a regular basis.
 *
 * <p>Ideally, Connect would implement a metadata topic where it publishes all
 * this information on a push basis, so we don't need to poll kafka or schema
 * registry.</p>
 */
@ThreadSafe
final class ConnectPollingService extends AbstractExecutionThreadService  {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectPollingService.class);
  private static final int MAX_INTERVAL_S = 30;
  private static final int START_INTERVAL_S = 1;
  private static final Connector STOP_SENTINEL =
      new Connector("_stop_", ignored -> false, Function.identity(), DataSourceType.KSTREAM, "");

  private final KsqlExecutionContext executionContext;
  private final Consumer<CreateSource> sourceCallback;
  private final int maxPollingIntervalSecs;
  private final AtomicInteger pollingIntervalSecs;

  // we use a blocking queue as a thread safe buffer between this class
  // and others that may be calling #addConnector(Connector) - this also
  // allows us to notify when a connector was added.
  private BlockingQueue<Connector> connectorQueue;
  private Set<Connector> connectors;

  ConnectPollingService(
      final KsqlExecutionContext executionContext,
      final Consumer<CreateSource> sourceCallback
  ) {
    this(executionContext, sourceCallback, MAX_INTERVAL_S);
  }

  @VisibleForTesting
  ConnectPollingService(
      final KsqlExecutionContext executionContext,
      final Consumer<CreateSource> sourceCallback,
      final int maxPollingIntervalSecs
  ) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
    this.sourceCallback = Objects.requireNonNull(sourceCallback, "sourceCallback");
    this.connectors = new HashSet<>();
    this.connectorQueue = new LinkedBlockingDeque<>();
    this.maxPollingIntervalSecs = maxPollingIntervalSecs;
    this.pollingIntervalSecs = new AtomicInteger(maxPollingIntervalSecs);
  }

  /**
   * Add this connector to the set of connectors that are polled by this
   * {@code ConnectPollingService}. Next time an iteration is scheduled in
   * {@value #MAX_INTERVAL_S} seconds, the connector will be included in the topic
   * scan.
   *
   * @param connector a connector to register
   */
  void addConnector(final Connector connector) {
    connectorQueue.add(connector);
    pollingIntervalSecs.set(START_INTERVAL_S);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      final Connector connector = connectorQueue.poll(
          pollingIntervalSecs.getAndUpdate(old -> Math.min(MAX_INTERVAL_S, 2 * old)),
          TimeUnit.SECONDS);
      if (connector == STOP_SENTINEL || connectorQueue.removeIf(c -> c == STOP_SENTINEL)) {
        return;
      } else if (connector != null) {
        connectors.add(connector);
      }

      drainQueue();
      runOneIteration();
    }
  }

  @VisibleForTesting
  void drainQueue() {
    connectorQueue.drainTo(connectors);
  }

  @VisibleForTesting
  void runOneIteration() {
    // avoid making external calls if unnecessary
    if (connectors.isEmpty()) {
      return;
    }

    try {
      final Set<String> topics = executionContext.getServiceContext()
          .getAdminClient()
          .listTopics()
          .names()
          .get(10, TimeUnit.SECONDS);

      final Set<String> subjects = ImmutableSet.copyOf(
          executionContext.getServiceContext()
              .getSchemaRegistryClient()
              .getAllSubjects()
      );

      for (final String topic : topics) {
        final Optional<Connector> maybeConnector = connectors
            .stream()
            .filter(candidate -> candidate.matches(topic))
            .findFirst();
        maybeConnector.ifPresent(connector -> handleTopic(topic, subjects, connector));
      }
    } catch (final Exception e) {
      LOG.error("Could not resolve connect sources. Trying again in at most {} seconds.",
          pollingIntervalSecs.get(),
          e);
    }
  }

  @Override
  protected void triggerShutdown() {
    // add the sentinel to the queue so that any blocking operation
    // gets resolved
    connectorQueue.add(STOP_SENTINEL);
  }

  private void handleTopic(
      final String topic,
      final Set<String> subjects,
      final Connector connector
  ) {
    final String valueSubject = topic + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
    if (subjects.contains(valueSubject)) {
      final String name = connector.getName();
      final String source = connector.mapToSource(topic).toUpperCase();

      // if the meta store already contains the source, don't send the extra command
      // onto the command topic
      if (executionContext.getMetaStore().getSource(source) == null) {
        LOG.info("Auto-Registering topic {} from connector {} as source {}",
            topic, connector.getName(), source);
        final Builder<String, Literal> builder = ImmutableMap.<String, Literal>builder()
            .put(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(topic))
            .put(CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("AVRO"))
            .put(CreateConfigs.SOURCE_CONNECTOR, new StringLiteral(name));

        connector.getKeyField().ifPresent(
            key -> builder.put(CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral(key))
        );

        final CreateSourceProperties properties = CreateSourceProperties.from(builder.build());
        final CreateSource createSource =
            connector.getSourceType() == (DataSourceType.KSTREAM)
                ? new CreateStream(QualifiedName.of(source), TableElements.of(), true, properties)
                : new CreateTable(QualifiedName.of(source), TableElements.of(), true, properties);

        sourceCallback.accept(createSource);
      }
    }
  }

}
