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
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import java.util.Collection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListTopicsExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldListKafkaTopics() {
    // Given:
    engine.givenKsqlTopic("topic1");
    engine.givenKafkaTopic("topic2");

    final AdminClient mockAdminClient = mock(AdminClient.class);
    final ListConsumerGroupsResult result = mock(ListConsumerGroupsResult.class);
    final KafkaFutureImpl<Collection<ConsumerGroupListing>> groups = new KafkaFutureImpl<>();

    when(result.all()).thenReturn(groups);
    when(mockAdminClient.listConsumerGroups()).thenReturn(result);
    groups.complete(ImmutableList.of());

    final ServiceContext serviceContext = TestServiceContext.create(
        engine.getServiceContext().getKafkaClientSupplier(),
        mockAdminClient,
        engine.getServiceContext().getTopicClient(),
        engine.getServiceContext().getSchemaRegistryClientFactory()
    );

    // When:
    final KafkaTopicsList topicsList =
        (KafkaTopicsList) CustomExecutors.LIST_TOPICS.execute(
            engine.prepare("LIST TOPICS;"),
            engine.getEngine(),
            serviceContext,
            engine.getKsqlConfig(),
            ImmutableMap.of()).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", true, ImmutableList.of(1), 0, 0),
        new KafkaTopicInfo("topic2", false, ImmutableList.of(1), 0, 0)
    ));
  }

}
