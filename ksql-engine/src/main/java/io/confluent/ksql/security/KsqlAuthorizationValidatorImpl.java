/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.security;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.kafka.common.acl.AclOperation;

/**
 * This authorization implementation checks if the user can perform Kafka and/or SR operations
 * on the topics or schemas found in the specified KSQL {@link Statement}.
 * </p>
 * This validator only works on Kakfa 2.3 or later.
 */
public class KsqlAuthorizationValidatorImpl implements KsqlAuthorizationValidator {
  @Override
  public void checkAuthorization(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final Statement statement
  ) {
    if (statement instanceof Query) {
      validateQuery(serviceContext, metaStore, (Query)statement);
    } else if (statement instanceof InsertInto) {
      validateInsertInto(serviceContext, metaStore, (InsertInto)statement);
    } else if (statement instanceof CreateAsSelect) {
      validateCreateAsSelect(serviceContext, metaStore, (CreateAsSelect)statement);
    } else if (statement instanceof PrintTopic) {
      validatePrintTopic(serviceContext, (PrintTopic)statement);
    } else if (statement instanceof CreateSource) {
      validateCreateSource(serviceContext, (CreateSource)statement);
    }
  }

  private void validateQuery(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final Query query
  ) {
    for (KsqlTopic topic : extractQueryTopics(query, metaStore)) {
      checkTopicAccess(
          topic.getKafkaTopicName(),
          AclOperation.READ,
          serviceContext.getTopicClient()
      );
    }
  }

  private void validateCreateAsSelect(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    /*
     * Check topic access for CREATE STREAM/TABLE AS SELECT statements.
     *
     * Validates Write on the target topic if exists, and Read on the query sources topics.
     *
     * The Create access is validated by the TopicCreateInjector which will attempt to create
     * the target topic using the same ServiceContext used for validation.
     */

    validateQuery(serviceContext, metaStore, createAsSelect.getQuery());

    // At this point, the topic should have been created by the TopicCreateInjector
    final String kafkaTopic = getCreateAsSelectSinkTopic(metaStore, createAsSelect);
    checkTopicAccess(kafkaTopic, AclOperation.WRITE, serviceContext.getTopicClient());
    checkSchemaAccess(kafkaTopic, AclOperation.WRITE, serviceContext.getSchemaRegistryClient());
  }

  private void validateInsertInto(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final InsertInto insertInto
  ) {
    /*
     * Check topic access for INSERT INTO statements.
     *
     * Validates Write on the target topic, and Read on the query sources topics.
     */

    validateQuery(serviceContext, metaStore, insertInto.getQuery());

    final String kafkaTopic = getSinkTopicName(metaStore, insertInto.getTarget().name());
    checkTopicAccess(kafkaTopic, AclOperation.WRITE, serviceContext.getTopicClient());
  }

  private void validatePrintTopic(
      final ServiceContext serviceContext,
      final PrintTopic printTopic
  ) {
    checkTopicAccess(
        printTopic.getTopic().toString(),
        AclOperation.READ,
        serviceContext.getTopicClient()
    );
  }

  private void validateCreateSource(
      final ServiceContext serviceContext,
      final CreateSource createSource
  ) {
    final String sourceTopic = createSource.getProperties().getKafkaTopic();
    checkTopicAccess(sourceTopic, AclOperation.READ, serviceContext.getTopicClient());

    if (createSource.getProperties().getValueFormat() == Format.AVRO) {
      checkSchemaAccess(sourceTopic, AclOperation.READ, serviceContext.getSchemaRegistryClient());
    }
  }

  private String getSinkTopicName(final MetaStore metaStore, final String streamOrTable) {
    final DataSource<?> dataSource = metaStore.getSource(streamOrTable);
    if (dataSource == null) {
      throw new KsqlException("Cannot validate for topic access from an unknown stream/table: "
          + streamOrTable);
    }

    return dataSource.getKafkaTopicName();
  }

  private void checkTopicAccess(
      final String topicName,
      final AclOperation operation,
      final KafkaTopicClient topicClient
  ) {
    final Set<AclOperation> authorizedOperations = topicClient.describeTopic(topicName)
        .authorizedOperations();

    // Kakfa 2.2 or lower do not support authorizedOperations(). In case of running on a
    // unsupported broker version, then the authorizeOperation will be null.
    if (authorizedOperations != null && !authorizedOperations.contains(operation)) {
      throw new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName));
    }
  }

  private void checkSchemaAccess(
      final String topicName,
      final AclOperation operation,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final String subject = topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

    try {
      schemaRegistryClient.getLatestSchemaMetadata(subject);
    } catch (final RestClientException e) {
      switch (e.getStatus()) {
        case HttpStatus.SC_UNAUTHORIZED:
        case HttpStatus.SC_FORBIDDEN:
          throw new KsqlSchemaAuthorizationException(operation, Collections.singleton(subject));
        default:
          // Do nothing. We assume the NOT FOUND and other errors are caught and  displayed
          // in different place
      }
    } catch (final Exception e) {
      throw new KsqlException(e);
    }
  }

  private String getCreateAsSelectSinkTopic(
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    return createAsSelect.getProperties().getKafkaTopic()
        .orElseGet(() -> getSinkTopicName(metaStore, createAsSelect.getName().name()));
  }

  private Set<KsqlTopic> extractQueryTopics(final Query query, final MetaStore metaStore) {
    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(query, null);
    return extractor.getKsqlTopics();
  }
}
