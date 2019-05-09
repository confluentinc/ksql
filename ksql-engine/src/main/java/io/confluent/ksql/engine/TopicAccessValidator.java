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

import avro.shaded.com.google.common.collect.Sets;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlTopicAccessException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;

/**
 * Wraps a {@link ServiceContext} to validate access to Kafka topics.
 */
public final class TopicAccessValidator {
  private final ServiceContext serviceContext;

  private TopicAccessValidator(final ServiceContext serviceContext) {
    this.serviceContext = serviceContext;
  }

  /**
   * Creates a {@code TopicAccessValidator} that uses the specified {@code ServiceContext}
   * to check Kafka topic access.
   *
   * @param serviceContext The {@code ServiceContext} to use.
   * @return A {@code TopicAccessValidator} object.
   */
  public static TopicAccessValidator as(final ServiceContext serviceContext) {
    return new TopicAccessValidator(serviceContext);
  }

  /**
   * Verifies if all query source topics have read access.
   *
   * @param node The node generated by the {@code QueryEngine#buildLogicalPlan}.
   * @throws KsqlTopicAccessException If a topic does not exist or does not have permissions.
   */
  public void verifyQuerySourcesPermissions(final PlanNode node) throws KsqlTopicAccessException {
    final Set<String> kafkaTopics = new HashSet<>();
    collectKafkaTopics(kafkaTopics, node);

    for (String kafkaTopic : kafkaTopics) {
      checkAccess(kafkaTopic, Collections.singleton(AclOperation.READ));
    }
  }

  /**
   * Verifies if a non-query KSQL statement has permissions to access the required topics.
   *
   * @param statement The statement to verify for permissions.
   * @throws KsqlTopicAccessException If a topic does not exist or does not have permissions.
   */
  public void verifyStatementPermissions(final Statement statement)
      throws KsqlTopicAccessException {
    if (statement instanceof CreateSource) {
      verifyCreateSourcePermissions((CreateSource) statement);
    } else if (statement instanceof InsertInto) {
      verifyInsertIntoPermissions((InsertInto) statement);
    } else if (statement instanceof InsertValues) {
      verifyInsertValuesPermissions((InsertValues) statement);
    } else if (statement instanceof CreateAsSelect) {
      verifyCreateAsSelectPermissions((CreateAsSelect) statement);
    }
  }

  private void verifyCreateAsSelectPermissions(final CreateAsSelect createAsSelect) {
    /*
     * Check permissions for CREATE STREAM and CREATE TABLE statements that have
     * a persistent query in it.
     *
     * This only validates the user can CREATE and WRITE the target topic. The persistent query
     * should be validated by verifyQuerySourcesPermissions() when the logical plan is
     * created (see EngineExecutor.execute)
     */

    final String kafkaTopic = createAsSelect.getName().getSuffix();
    checkAccess(kafkaTopic, Sets.newHashSet(AclOperation.CREATE, AclOperation.WRITE));
  }

  private void verifyInsertIntoPermissions(final InsertInto insertInto) {
    /*
     * Check permissions for INSERT INTO statements that have a persistent query in it.
     *
     * This only validates the user can WRITE the target topic. The persistent query
     * should be validated by verifyQuerySourcesPermissions() when the logical plan is
     * created (see EngineExecutor.execute)
     */

    final String kafkaTopic = insertInto.getTarget().getSuffix();
    checkAccess(kafkaTopic, Collections.singleton(AclOperation.WRITE));
  }

  private void verifyInsertValuesPermissions(final InsertValues insertValues) {
    /*
     * Check permissions for INSERT INTO VALUES statements that do not have
     * a persistent query in it.
     */

    final String kafkaTopic = insertValues.getTarget().getSuffix();
    checkAccess(kafkaTopic, Collections.singleton(AclOperation.WRITE));
  }

  private void verifyCreateSourcePermissions(final CreateSource createSource) {
    /*
     * Check permissions for CREATE STREAM and CREATE TABLE statements that do not have
     * a persistent query in it. These statements come the WITH kafka_topic property.
     */
    final String kafkaTopic = createSource.getProperties().getKafkaTopic();
    checkAccess(kafkaTopic, Collections.singleton(AclOperation.READ));
  }

  /**
   * Walks through the PlanNode to find all the data sources of the query node.
   */
  private void collectKafkaTopics(final Set<String> kafkaTopics, final PlanNode node) {
    if (node == null) {
      return;
    }

    if (node instanceof DataSourceNode) {
      kafkaTopics.add(((DataSourceNode) node).getDataSource().getKafkaTopicName());
      return;
    }

    for (PlanNode nodeSource : node.getSources()) {
      collectKafkaTopics(kafkaTopics, nodeSource);
    }
  }

  /**
   * Checks if the ServiceContext has access to the topic with the specified AclOperation list.
   */
  private void checkAccess(final String topicName, final Set<AclOperation> operations) {
    try {
      final TopicDescription topicDescription =
          serviceContext.getTopicClient().describeTopic(topicName);

      if (!operations.stream().allMatch(topicDescription.authorizedOperations()::contains)) {
        throw new KsqlTopicAccessException(topicName);
      }
    } catch (final Exception e) {
      throw new KsqlTopicAccessException(topicName, e);
    }
  }
}
