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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlBackendAccessValidatorTest {
  private final static String TOPIC_NAME_1 = "topic1";
  private final static String TOPIC_NAME_2 = "topic2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  private KsqlSecurityContext securityContext;
  private KsqlAccessValidator accessValidator;

  @Before
  public void setUp() {
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    givenTopic(TOPIC_NAME_1, TOPIC_1);
    givenTopic(TOPIC_NAME_2, TOPIC_2);

    accessValidator = new KsqlBackendAccessValidator();
  }

  @Test
  public void shouldAllowIfAuthorizedOperationsIsNull() {
    // Checks compatibility with unsupported Kafka authorization checks

    // Given:
    givenTopicPermissions(TOPIC_1, null);

    // When/Then:
    accessValidator.checkTopicAccess(securityContext, TOPIC_NAME_1, AclOperation.READ);
  }

  @Test
  public void shouldAllowIfAuthorizedOperationsContainsREAD() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));

    // When/Then:
    accessValidator.checkTopicAccess(securityContext, TOPIC_NAME_1, AclOperation.READ);
  }

  @Test
  public void shouldDenyIfAuthorizedOperationsDoesNotContainREAD() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> accessValidator.checkTopicAccess(securityContext, TOPIC_NAME_1, AclOperation.READ)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    )));
  }

  @Test
  public void shouldThrowExceptionWhenDescribeTopicFails() {
    // Given:
    when(kafkaTopicClient.describeTopic(TOPIC_NAME_1))
        .thenThrow(KafkaResponseGetFailedException.class);

    // When:
    assertThrows(
        KafkaResponseGetFailedException.class,
        () -> accessValidator.checkTopicAccess(securityContext, TOPIC_NAME_1, AclOperation.READ)
    );
  }

  private void givenTopic(final String topicName, final TopicDescription topicDescription) {
    when(topicDescription.name()).thenReturn(topicName);
    when(kafkaTopicClient.describeTopic(topicDescription.name())).thenReturn(topicDescription);
  }

  private void givenTopicPermissions(
      final TopicDescription topicDescription,
      final Set<AclOperation> operations
  ) {
    when(topicDescription.authorizedOperations()).thenReturn(operations);
  }
}
