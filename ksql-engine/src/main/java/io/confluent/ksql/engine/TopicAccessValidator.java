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

package io.confluent.ksql.engine;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.ksql.util.KsqlException;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.acl.AclOperation;

/**
 * Checks if a {@link ServiceContext} has access to the source and target topics of transient
 * and persistent query statements.
 */
public class TopicAccessValidator {
  private final ServiceContext serviceContext;
  private final MetaStore metaStore;
  private final SourceTopicsExtractor sourceTopicsExtractor;

  public TopicAccessValidator(final ServiceContext serviceContext, final MetaStore metaStore) {
    this.serviceContext = serviceContext;
    this.metaStore = metaStore;
    this.sourceTopicsExtractor = new SourceTopicsExtractor(metaStore);
  }

  /**
   * Checks if the {@link ServiceContext} has access to the source and target topics of transient
   * and persistent query statements.
   * </p>
   * It checks for Read access to all the source topics and Write access to the target topic.
   * </p>
   * Other permissions checks are already validated by the {@code Injectors} which attempts to
   * create and/or delete topics using the same {@code ServiceContext}.
   *
   * @param statement The statement to verify for permissions.
   * @throws KsqlException If a topic is not authorized for access, or the topic does not exist.
   */
  public void validate(final Statement statement) {
    if (statement instanceof Query) {
      validateQueryTopicSources((Query)statement);
    } else if (statement instanceof InsertInto) {
      validateInsertInto((InsertInto)statement);
    } else if (statement instanceof CreateAsSelect) {
      validateCreateAsSelect((CreateAsSelect)statement);
    }
  }

  private void validateQueryTopicSources(final Query query) {
    sourceTopicsExtractor.process(query, null);
    for (String kafkaTopic : sourceTopicsExtractor.getSourceTopics()) {
      checkAccess(kafkaTopic, AclOperation.READ);
    }
  }

  private void validateCreateAsSelect(final CreateAsSelect createAsSelect) {
    /*
     * Check topic access for CREATE STREAM/TABLE AS SELECT statements.
     *
     * Validates Write on the target topic if exists, and Read on the query sources topics.
     *
     * The Create access is validated by the TopicCreateInjector which will attempt to create
     * the target topic using the same ServiceContext used for validation.
     */

    validateQueryTopicSources(createAsSelect.getQuery());

    String kafkaTopic;

    try {
      kafkaTopic = getSourceTopicName(createAsSelect.getName().getSuffix());
    } catch (final KsqlException e) {
      kafkaTopic = createAsSelect.getName().getSuffix();
    }

    // At this point, the topic should have been created by the TopicCreateInjector
    checkAccess(kafkaTopic, AclOperation.WRITE);
  }

  private void validateInsertInto(final InsertInto insertInto) {
    /*
     * Check topic access for INSERT INTO statements.
     *
     * Validates Write on the target topic, and Read on the query sources topics.
     */

    validateQueryTopicSources(insertInto.getQuery());

    final String kafkaTopic = getSourceTopicName(insertInto.getTarget().getSuffix());
    checkAccess(kafkaTopic, AclOperation.WRITE);
  }

  private String getSourceTopicName(final String ksqlTopicName) {
    final DataSource<?> dataSource = metaStore.getSource(ksqlTopicName);
    if (dataSource == null) {
      throw new KsqlException("Cannot validate for topic access from an unknown stream/table: "
          + ksqlTopicName);
    }

    return dataSource.getKafkaTopicName();
  }

  /**
   * Checks if the ServiceContext has access to the topic with the specified AclOperation.
   */
  private void checkAccess(final String topicName, final AclOperation operation) {
    final Set<AclOperation> authorizedOperations = serviceContext.getTopicClient()
        .describeTopic(topicName).authorizedOperations();

    if (!authorizedOperations.contains(operation)) {
      // This error message is similar to what Kafka throws when it cannot access the topic
      // due to an authorization error. I used this message to keep a consistent message.
      throw new KsqlException(String.format(
              "Failed to %s Kafka topic: [%s]%n"
                  + "Caused by: Not authorized to access topic",
              StringUtils.capitalize(operation.toString().toLowerCase()), topicName)
      );
    }
  }
}
