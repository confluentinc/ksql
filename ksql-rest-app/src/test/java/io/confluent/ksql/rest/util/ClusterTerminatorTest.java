/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.util;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusterTerminatorTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private PersistentQueryMetadata persistentQueryMetadata;
  @Mock
  private QueryId queryId;
  @Mock
  private MetaStore metaStore;
  @Mock
  private KsqlTopic ksqlTopic;

  private ClusterTerminator clusterTerminator;

  @Before
  public void setup() {
    clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine);
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(persistentQueryMetadata));
    when(persistentQueryMetadata.getQueryId()).thenReturn(queryId);
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("command_topic");
    when(ksqlEngine.getTopicClient()).thenReturn(kafkaTopicClient);
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    when(metaStore.getAllKsqlTopics()).thenReturn(ImmutableMap.of("FOO", getKsqlTopic("FOO", "K_FOO", true)));
  }

  @Test
  public void shouldTerminatePersistetQueries() throws Exception {
    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(ksqlEngine).terminateQuery(same(queryId), eq(true));
    verify(ksqlEngine).close();
  }


  @Test
  public void shouldDeleteTopicListWithExplicitTopicName() {
    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_FOO"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_FOO"));
  }

  @Test
  public void shouldNotDeleteTopicNonSinkTopic() {
    // Given:

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.emptyList());
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() {
    // Given:

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.emptyList());
  }


  @Test
  public void shouldDeleteTopicListWithPattern() {
    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_FO*"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_FOO"));
  }

  @Test
  public void shouldDeleteCommandTopic() {
    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
  }

  private KsqlTopic getKsqlTopic(final String topicName, final String kafkaTopicName, final boolean isSink) {
    return new KsqlTopic(topicName, kafkaTopicName, null, isSink);
  }
}
