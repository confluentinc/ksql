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

import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Set;
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
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final Statement statement
  ) {
    if (statement instanceof Query) {
      validateQuery(securityContext, metaStore, (Query)statement);
    } else if (statement instanceof InsertInto) {
      validateInsertInto(securityContext, metaStore, (InsertInto)statement);
    } else if (statement instanceof CreateAsSelect) {
      validateCreateAsSelect(securityContext, metaStore, (CreateAsSelect)statement);
    } else if (statement instanceof PrintTopic) {
      validatePrintTopic(securityContext, (PrintTopic)statement);
    } else if (statement instanceof CreateSource) {
      validateCreateSource(securityContext, (CreateSource)statement);
    }
  }

  private void validateQuery(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final Query query
  ) {
    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(query, null);
    for (String kafkaTopic : extractor.getSourceTopics()) {
      checkAccess(securityContext, kafkaTopic, AclOperation.READ);
    }
  }

  private void validateCreateAsSelect(
      final KsqlSecurityContext securityContext,
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

    validateQuery(securityContext, metaStore, createAsSelect.getQuery());

    // At this point, the topic should have been created by the TopicCreateInjector
    final String kafkaTopic = getCreateAsSelectSinkTopic(metaStore, createAsSelect);
    checkAccess(securityContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validateInsertInto(
      final KsqlSecurityContext securityContext,
      final MetaStore metaStore,
      final InsertInto insertInto
  ) {
    /*
     * Check topic access for INSERT INTO statements.
     *
     * Validates Write on the target topic, and Read on the query sources topics.
     */

    validateQuery(securityContext, metaStore, insertInto.getQuery());

    final String kafkaTopic = getSourceTopicName(metaStore, insertInto.getTarget());
    checkAccess(securityContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validatePrintTopic(
          final KsqlSecurityContext securityContext,
          final PrintTopic printTopic
  ) {
    checkAccess(securityContext, printTopic.getTopic(), AclOperation.READ);
  }

  private void validateCreateSource(
      final KsqlSecurityContext securityContext,
      final CreateSource createSource
  ) {
    final String sourceTopic = createSource.getProperties().getKafkaTopic();
    checkAccess(securityContext, sourceTopic, AclOperation.READ);
  }

  private String getSourceTopicName(final MetaStore metaStore, final SourceName streamOrTable) {
    final DataSource<?> dataSource = metaStore.getSource(streamOrTable);
    if (dataSource == null) {
      throw new KsqlException("Cannot validate for topic access from an unknown stream/table: "
          + streamOrTable);
    }

    return dataSource.getKafkaTopicName();
  }

  /**
   * Checks if the ServiceContext has access to the topic with the specified AclOperation.
   */
  private void checkAccess(
      final KsqlSecurityContext securityContext,
      final String topicName,
      final AclOperation operation
  ) {
    final Set<AclOperation> authorizedOperations = securityContext.getServiceContext()
        .getTopicClient().describeTopic(topicName).authorizedOperations();

    // Kakfa 2.2 or lower do not support authorizedOperations(). In case of running on a
    // unsupported broker version, then the authorizeOperation will be null.
    if (authorizedOperations != null && !authorizedOperations.contains(operation)) {
      // This error message is similar to what Kafka throws when it cannot access the topic
      // due to an authorization error. I used this message to keep a consistent message.
      throw new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName));
    }
  }

  private String getCreateAsSelectSinkTopic(
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    return createAsSelect.getProperties().getKafkaTopic()
        .orElseGet(() -> getSourceTopicName(metaStore, createAsSelect.getName()));
  }
}
