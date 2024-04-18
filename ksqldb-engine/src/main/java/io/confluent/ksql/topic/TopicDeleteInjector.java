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

package io.confluent.ksql.topic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This {@code Injector} will delete the topic associated with a
 * {@code DROP [STREAM/TABLE] X DELETE TOPIC}. Any other statements are just
 * passed through. Furthermore, it will remove the DELETE TOPIC clause from
 * the statement, indicating that the operation has already been done.
 *
 * <p>If the topic being deleted is {@link FormatFactory#AVRO},
 * this injector will also clean up the corresponding schema in the schema
 * registry.
 */
public class TopicDeleteInjector implements Injector {

  private final MetaStore metastore;
  private final KafkaTopicClient topicClient;
  private final SchemaRegistryClient schemaRegistryClient;

  public TopicDeleteInjector(
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this(
        Objects.requireNonNull(executionContext, "executionContext").getMetaStore(),
        serviceContext.getTopicClient(),
        serviceContext.getSchemaRegistryClient()
    );
  }

  @VisibleForTesting
  TopicDeleteInjector(
      final MetaStore metastore,
      final KafkaTopicClient serviceContext,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    this.metastore = Objects.requireNonNull(metastore, "metastore");
    this.topicClient = Objects.requireNonNull(serviceContext, "topicClient");
    this.schemaRegistryClient =
        Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient");
  }

  @SuppressWarnings({"unchecked", "UnstableApiUsage"})
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    if (!(statement.getStatement() instanceof DropStatement)) {
      return statement;
    }

    final DropStatement dropStatement = (DropStatement) statement.getStatement();
    if (!dropStatement.isDeleteTopic()) {
      return statement;
    }

    final SourceName sourceName = dropStatement.getName();
    final DataSource source = metastore.getSource(sourceName);

    if (source != null) {
      if (source.isSource()) {
        throw new KsqlException("Cannot delete topic for read-only source: " + sourceName.text());
      }
      checkTopicRefs(source);

      deleteTopic(source);

      final Closer closer = Closer.create();
      closer.register(() -> deleteKeySubject(source));
      closer.register(() -> deleteValueSubject(source));
      try {
        closer.close();
      } catch (final KsqlException e) {
        throw e;
      } catch (final Exception e) {
        throw new KsqlException(e);
      }
    } else if (!dropStatement.getIfExists()) {
      throw new KsqlException("Could not find source to delete topic for: " + statement);
    }

    final T withoutDelete = (T) dropStatement.withoutDeleteClause();
    final String withoutDeleteText = SqlFormatter.formatSql(withoutDelete) + ";";

    return statement.withStatement(withoutDeleteText, withoutDelete);
  }

  private void deleteTopic(final DataSource source) {
    try {
      ExecutorUtil.executeWithRetries(
          () -> topicClient.deleteTopics(ImmutableList.of(source.getKafkaTopicName())),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new RuntimeException("Could not delete the corresponding kafka topic: "
          + source.getKafkaTopicName(), e);
    }
  }

  private void deleteKeySubject(final DataSource source) {
    try {
      final Format keyFormat = FormatFactory
          .fromName(source.getKsqlTopic().getKeyFormat().getFormat());

      if (keyFormat.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
        SchemaRegistryUtil.deleteSubjectWithRetries(
            schemaRegistryClient,
            KsqlConstants.getSRSubject(source.getKafkaTopicName(), true));
      }
    } catch (final Exception e) {
      checkSchemaError(e, source.getKafkaTopicName());
    }
  }

  private void deleteValueSubject(final DataSource source) {
    try {
      final Format valueFormat = FormatFactory
          .fromName(source.getKsqlTopic().getValueFormat().getFormat());

      if (valueFormat.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)) {
        SchemaRegistryUtil.deleteSubjectWithRetries(
            schemaRegistryClient,
            KsqlConstants.getSRSubject(source.getKafkaTopicName(), false));
      }
    } catch (final Exception e) {
      checkSchemaError(e, source.getKafkaTopicName());
    }
  }

  private static void checkSchemaError(final Exception error, final String sourceName) {
    if (!(SchemaRegistryUtil.isSubjectNotFoundErrorCode(error))) {
      throw new RuntimeException("Could not clean up the schema registry for topic: "
              + sourceName, error);
    }
  }

  private void checkTopicRefs(final DataSource source) {
    final String topicName = source.getKafkaTopicName();
    final SourceName sourceName = source.getName();
    final Map<SourceName, DataSource> sources = metastore.getAllDataSources();
    final String using = sources.values().stream()
        .filter(s -> s.getKafkaTopicName().equals(topicName))
        .map(DataSource::getName)
        .filter(name -> !sourceName.equals(name))
        .map(SourceName::text)
        .sorted()
        .collect(Collectors.joining(", "));
    if (!using.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "Refusing to delete topic. Found other data sources (%s) using topic %s",
              using,
              topicName
          )
      );
    }
  }
}
