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

import avro.shaded.com.google.common.collect.ImmutableList;
import avro.shaded.com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ClusterTerminatorTest {

  private KsqlConfig ksqlConfig = new KsqlConfig(
      Collections.singletonMap(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "foo"));
  private KsqlEngine ksqlEngine;
  private KafkaTopicClient kafkaTopicClient;
  private ClusterTerminator clusterTerminator;
  private PersistentQueryMetadata persistentQuery;
  private QueryId queryId;
  private MetaStore metaStore;

  @Before
  public void init() {
    queryId = EasyMock.niceMock(QueryId.class);
    persistentQuery = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(persistentQuery.getQueryId()).andReturn(queryId);
    ksqlEngine = EasyMock.niceMock(KsqlEngine.class);
    EasyMock.expect(ksqlEngine.terminateQuery(EasyMock.anyObject(QueryId.class), EasyMock.anyBoolean())).andReturn(true);
    kafkaTopicClient = EasyMock.niceMock(KafkaTopicClient.class);
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());

    givenEngineWith(ImmutableSet.of(EasyMock.niceMock(QueryMetadata.class)));

    clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine);
  }

  @Test
  public void shouldTellEngineToStopAcceptingStaetements() throws Exception {
    // Given:
    ksqlEngine.stopAcceptingStatements();
    EasyMock.expectLastCall();
    EasyMock.replay(ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    EasyMock.verify(ksqlEngine);
  }

  @Test
  public void shouldTerminatePersistetQueries() throws Exception {
    // Given:
    givenEngineWith(Collections.singleton(persistentQuery));
    EasyMock.replay(ksqlEngine, persistentQuery);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    EasyMock.verify(ksqlEngine, persistentQuery);
  }

  @Test
  public void shouldCloseNonPersistentQueries() throws Exception {
    // Given:
    givenEngineWith(Collections.singleton(EasyMock.niceMock(QueryMetadata.class)));
    EasyMock.replay(ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    EasyMock.verify(ksqlEngine);
  }

  @Test
  public void shouldDeleteTopicListWithExplicitTopicName() {
    // Given:
    givenSinkTopicsExist("bar", "barr", "foo");

    kafkaTopicClient.deleteTopics(ImmutableList.of("bar"));
    EasyMock.expectLastCall();
    EasyMock.replay(kafkaTopicClient, ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    EasyMock.verify(kafkaTopicClient);
  }

  @Test
  public void shouldNotDeleteTopicNonSinkTopic() {
    // Given:
    givenNonSinkTopicsExist("bar", "barr", "foo");

    kafkaTopicClient.deleteTopics(EasyMock.anyObject());
    EasyMock.expectLastCall().times(1); // <-- command topic _only_
    EasyMock.replay(kafkaTopicClient, ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    EasyMock.verify(kafkaTopicClient);
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() {
    // Given:
    givenSinkTopicsExist("bar", "barr", "foo");

    kafkaTopicClient.deleteTopics(EasyMock.anyObject());
    EasyMock.expectLastCall().times(1); // <-- command topic _only_
    EasyMock.replay(kafkaTopicClient, ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("Bar"));

    // Then:
    EasyMock.verify(kafkaTopicClient);
  }

  @Test
  public void shouldDeleteTopicListWithPattern() {
    // Given:
    givenSinkTopicsExist("bar", "barr", "foo");
    givenNonSinkTopicsExist("barrr");

    kafkaTopicClient.deleteTopics(ImmutableList.of("barr", "bar"));
    EasyMock.expectLastCall();
    EasyMock.replay(kafkaTopicClient, ksqlEngine);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar*"));

    // Then:
    EasyMock.verify(kafkaTopicClient);
  }

  @Test
  public void shouldDeleteCommandTopic() {
    final List<String> deleteTopicList = Collections.emptyList();
    kafkaTopicClient.deleteTopics(ImmutableList.of("_confluent-ksql-foo_command_topic"));
    EasyMock.expectLastCall().once();
    EasyMock.replay(ksqlEngine, kafkaTopicClient);
    clusterTerminator.terminateCluster(deleteTopicList);

    EasyMock.verify(ksqlEngine, kafkaTopicClient);
  }

  private void givenNonSinkTopicsExist(final String... topics) {
    Arrays.stream(topics)
        .map(name -> new KsqlTopic(name + "-internal", name, new KsqlJsonTopicSerDe(), false))
        .forEach(metaStore::putTopic);
  }

  private void givenSinkTopicsExist(final String... topics) {
    Arrays.stream(topics)
        .map(name -> new KsqlTopic(name + "-internal", name, new KsqlJsonTopicSerDe(), true))
        .forEach(metaStore::putTopic);
  }

  private void givenEngineWith(final Set<QueryMetadata> queries) {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getAllLiveQueries())
        .andReturn(queries)
        .anyTimes();

    EasyMock.expect(ksqlEngine.getTopicClient()).andReturn(kafkaTopicClient).anyTimes();
    EasyMock.expect(ksqlEngine.getMetaStore()).andReturn(metaStore).anyTimes();
  }
}
