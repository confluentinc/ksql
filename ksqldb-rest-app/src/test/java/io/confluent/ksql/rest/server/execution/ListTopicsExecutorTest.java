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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicInfoExtended;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KafkaTopicsListExtended;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import java.util.Collection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListTopicsExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();
  @Mock
  private AdminClient adminClient;

  private ServiceContext serviceContext;

  @Before
  public void setUp() {
     serviceContext = TestServiceContext.create(
          engine.getServiceContext().getKafkaClientSupplier(),
          adminClient,
          engine.getServiceContext().getTopicClient(),
          engine.getServiceContext().getSchemaRegistryClientFactory(),
          engine.getServiceContext().getConnectClient()
    );
  }

  @Test
  public void shouldListKafkaTopicsWithoutInternalTopics() {
    // Given:
    engine.givenKafkaTopic("topic1");
    engine.givenKafkaTopic("topic2");
    engine.givenKafkaTopic("_confluent_any_topic");

    // When:
    final KafkaTopicsList topicsList =
        (KafkaTopicsList) CustomExecutors.LIST_TOPICS.execute(
            engine.configure("LIST TOPICS;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            serviceContext
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", ImmutableList.of(1)),
        new KafkaTopicInfo("topic2", ImmutableList.of(1))
    ));
  }

  @Test
  public void shouldListKafkaTopicsIncludingInternalTopics() {
    // Given:
    engine.givenKafkaTopic("topic1");
    engine.givenKafkaTopic("topic2");
    engine.givenKafkaTopic("_confluent_any_topic");

    // When:
    final KafkaTopicsList topicsList =
        (KafkaTopicsList) CustomExecutors.LIST_TOPICS.execute(
            engine.configure("LIST ALL TOPICS;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            serviceContext
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", ImmutableList.of(1)),
        new KafkaTopicInfo("topic2", ImmutableList.of(1)),
        new KafkaTopicInfo("_confluent_any_topic", ImmutableList.of(1))
    ));
  }

  @Test
  public void shouldListKafkaTopicsThatDifferByCase() {
    // Given:
    engine.givenKafkaTopic("topic1");
    engine.givenKafkaTopic("toPIc1");

    // When:
    final KafkaTopicsList topicsList =
        (KafkaTopicsList) CustomExecutors.LIST_TOPICS.execute(
            engine.configure("LIST TOPICS;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            serviceContext
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", ImmutableList.of(1)),
        new KafkaTopicInfo("toPIc1", ImmutableList.of(1))
    ));
  }

  @Test
  public void shouldListKafkaTopicsExtended() {
    // Given:
    engine.givenKafkaTopic("topic1");
    engine.givenKafkaTopic("topic2");

    final ListConsumerGroupsResult result = mock(ListConsumerGroupsResult.class);
    final KafkaFutureImpl<Collection<ConsumerGroupListing>> groups = new KafkaFutureImpl<>();

    when(result.all()).thenReturn(groups);
    when(adminClient.listConsumerGroups()).thenReturn(result);
    groups.complete(ImmutableList.of());

    // When:
    final KafkaTopicsListExtended topicsList =
        (KafkaTopicsListExtended) CustomExecutors.LIST_TOPICS.execute(
            engine.configure("LIST TOPICS EXTENDED;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            serviceContext
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfoExtended("topic1", ImmutableList.of(1), 0, 0),
        new KafkaTopicInfoExtended("topic2", ImmutableList.of(1), 0, 0)
    ));
  }
}
