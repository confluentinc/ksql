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

import com.google.common.collect.Sets;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlTopicAccessException;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicAccessValidatorTest {
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private TopicAccessValidator accessValidator;

  @Before
  public void setUp() {
    accessValidator = TopicAccessValidator.from(serviceContext);
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
  }

  @Test
  public void shouldAllowInsertIntoWithWriteOperations() {
    // Given:
    final Statement statement = insertInto("topic1");
    givenTopicPermissions("topic1", Collections.singleton(AclOperation.WRITE));

    // When:
    accessValidator.checkTargetTopicsPermissions(statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldDenyInsertIntoWithNonWriteOperations() {
    // Given:
    final Statement statement = insertInto("topic1");
    givenTopicPermissions("topic1", Sets.newHashSet(
        AclOperation.CREATE, AclOperation.READ, AclOperation.DELETE
    ));

    // Then:
    expectedException.expect(KsqlTopicAccessException.class);

    // When:
    accessValidator.checkTargetTopicsPermissions(statement);
  }

  @Test
  public void shouldAllowCreateAsSelectWithCreateWriteOperations() {
    // Given:
    final Statement statement = createAsSelect("topic1");
    givenTopicPermissions("topic1", Sets.newHashSet(
        AclOperation.CREATE, AclOperation.WRITE
    ));

    // When:
    accessValidator.checkTargetTopicsPermissions(statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowExceptionWhenTopicClientFails() {
    // Given:
    final Statement statement = insertInto("topic1");
    givenTopicClientError("topic1");

    // Then:
    expectedException.expect(KafkaResponseGetFailedException.class);

    // When:
    accessValidator.checkTargetTopicsPermissions(statement);
  }

  @Test
  public void shouldAllowQuerySourcesWithReadOperations() {
    // Given:
    final PlanNode planNode = createPlan(Sets.newHashSet("topic1", "topic2"));
    givenTopicPermissions("topic1", Sets.newHashSet(AclOperation.READ));
    givenTopicPermissions("topic2", Sets.newHashSet(AclOperation.READ));

    // When:
    accessValidator.checkSourceTopicsPermissions(planNode);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldDenyQuerySourcesWithNonReadOperations() {
    // Given:
    final PlanNode planNode = createPlan(Sets.newHashSet("topic1", "topic2"));
    givenTopicPermissions("topic1", Sets.newHashSet(AclOperation.READ));
    givenTopicPermissions("topic2", Sets.newHashSet(AclOperation.WRITE));

    // Then:
    expectedException.expect(KsqlTopicAccessException.class);

    // When:
    accessValidator.checkSourceTopicsPermissions(planNode);
  }

  private PlanNode createPlan(final Set<String> sourceTopics) {
    final PlanNode planNode = mock(PlanNode.class);

    when(planNode.getAllSourceKafkaTopics()).thenReturn(sourceTopics);

    return planNode;
  }

  private Statement createAsSelect(final String targetTopic) {
    final CreateAsSelect createAsSelect = mock(CreateAsSelect.class);

    when(createAsSelect.getName()).thenReturn(QualifiedName.of(targetTopic));

    return createAsSelect;
  }

  private Statement insertInto(final String targetTopic) {
    final InsertInto insertInto = mock(InsertInto.class);

    when(insertInto.getTarget()).thenReturn(QualifiedName.of(targetTopic));

    return insertInto;
  }

  private void givenTopicPermissions(final String topic, final Set<AclOperation> operations) {
    final TopicDescription topicDescription = mock(TopicDescription.class);

    when(kafkaTopicClient.describeTopic(topic)).thenReturn(topicDescription);
    when(topicDescription.authorizedOperations()).thenReturn(operations);
  }

  private void givenTopicClientError(final String topic) {
    doThrow(KafkaResponseGetFailedException.class).when(kafkaTopicClient).describeTopic(topic);
  }
}
