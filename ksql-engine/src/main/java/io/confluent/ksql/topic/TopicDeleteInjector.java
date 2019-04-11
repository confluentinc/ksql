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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.ExecutorUtil.RetryBehaviour;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

/**
 * This {@code Injector} will delete the topic associated with a
 * {@code DROP [STREAM/TABLE] X DELETE TOPIC}. Any other statements are just
 * passed through. Furthermore, it will remove the DELETE TOPIC clause from
 * the statement, indicating that the operation has already been done.
 *
 * <p>If the topic being deleted is {@link DataSource.DataSourceSerDe#AVRO},
 * this injector will also clean up the corresponding schema in the schema
 * registry.
 */
public class TopicDeleteInjector implements Injector {

  private final MetaStore metastore;
  private final KafkaTopicClient topicClient;
  private final SchemaRegistryClient schemaRegistryClient;

  public TopicDeleteInjector(final KsqlExecutionContext executionContext) {
    this(
        Objects.requireNonNull(executionContext, "executionContext").getMetaStore(),
        executionContext.getServiceContext().getTopicClient(),
        executionContext.getServiceContext().getSchemaRegistryClient()
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

  @SuppressWarnings("unchecked")
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

    final String sourceName = dropStatement.getName().getSuffix();
    final StructuredDataSource<?> source = metastore.getSource(sourceName);

    if (source != null) {
      try {
        ExecutorUtil.executeWithRetries(
            () -> topicClient.deleteTopics(ImmutableList.of(source.getKafkaTopicName())),
            RetryBehaviour.ALWAYS);
      } catch (Exception e) {
        throw new KsqlException("Could not delete the corresponding kafka topic: "
            + source.getKafkaTopicName(), e);
      }

      if (source.isSerdeFormat(DataSource.DataSourceSerDe.AVRO)) {
        try {
          SchemaRegistryUtil.deleteSubjectWithRetries(
              schemaRegistryClient,
              source.getKafkaTopicName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
        } catch (final Exception e) {
          throw new KsqlException("Could not clean up the schema registry for topic: "
              + source.getKafkaTopicName(), e);
        }
      }
    } else if (dropStatement.getIfExists()) {
      throw new KsqlException("Could not find source to delete topic for: " + statement);
    }

    final T withoutDelete = (T) dropStatement.withoutDeleteClause();
    final String withoutDeleteText = SqlFormatter.formatSql(withoutDelete) + ";";

    return statement.withStatement(withoutDeleteText, withoutDelete);
  }
}
