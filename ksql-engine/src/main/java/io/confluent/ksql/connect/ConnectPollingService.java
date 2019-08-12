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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
final class ConnectPollingService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectPollingService.class);
  private static final int INTERVAL_S = 30;

  private final KsqlExecutionContext executionContext;
  private final Consumer<CreateSource> sourceCallback;

  private Set<Connector> connectors;

  ConnectPollingService(
      final KsqlExecutionContext executionContext,
      final Consumer<CreateSource> sourceCallback
  ) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
    this.sourceCallback = Objects.requireNonNull(sourceCallback, "sourceCallback");
    this.connectors = ConcurrentHashMap.newKeySet();
  }

  /**
   * Add this connector to the set of connectors that are polled by this
   * {@code ConnectPollingService}. Next time an iteration is scheduled in
   * {@value #INTERVAL_S} seconds, the connector will be included in the topic
   * scan.
   *
   * @param connector a connector to register
   */
  void addConnector(final Connector connector) {
    connectors.add(connector);
  }

  @Override
  protected void runOneIteration() {
    if (connectors.isEmpty()) {
      // avoid making external calls if unnecessary
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
      LOG.error("Could not resolve connect sources. Trying again in {} seconds.", INTERVAL_S, e);
    }
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

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, INTERVAL_S, TimeUnit.SECONDS);
  }
}
