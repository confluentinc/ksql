/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListRegisteredTopicsExecutorTest extends CustomExecutorsTest {

  @Test
  public void shouldListRegisteredTopics() {
    // Given:
    final KsqlTopic topic1 = givenKsqlTopic("topic1");
    final KsqlTopic topic2 = givenKsqlTopic("topic2");

    // When:
    final KsqlTopicsList topicsList =
        (KsqlTopicsList) CustomExecutors.LIST_REGISTERED_TOPICS.execute(
            prepare("LIST REGISTERED TOPICS;"),
            engine,
            serviceContext,
            ksqlConfig,
            ImmutableMap.of()).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(),
        contains(new KsqlTopicInfo(topic1), new KsqlTopicInfo(topic2)));
  }


}
