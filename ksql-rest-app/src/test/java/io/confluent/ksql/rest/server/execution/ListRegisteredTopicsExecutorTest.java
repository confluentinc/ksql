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
import static org.hamcrest.Matchers.contains;

import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListRegisteredTopicsExecutorTest {

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldListRegisteredTopics() {
    // Given:
    final KsqlTopic topic1 = engine.givenKsqlTopic("topic1");
    final KsqlTopic topic2 = engine.givenKsqlTopic("topic2");

    // When:
    final KsqlTopicsList topicsList =
        (KsqlTopicsList) CustomExecutors.LIST_REGISTERED_TOPICS.execute(
            engine.configure("LIST REGISTERED TOPICS;"),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(topicsList.getTopics(),
        contains(new KsqlTopicInfo(topic1), new KsqlTopicInfo(topic2)));
  }


}
