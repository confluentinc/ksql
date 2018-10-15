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

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ClusterTerminatorTest {

  private KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "foo"));
  private KsqlEngine ksqlEngine;
  private KafkaTopicClient kafkaTopicClient;

  @Before
  public void init() {
    ksqlEngine = EasyMock.mock(KsqlEngine.class);
    kafkaTopicClient = EasyMock.mock(KafkaTopicClient.class);
    EasyMock.expect(ksqlEngine.getTopicClient()).andReturn(kafkaTopicClient).anyTimes();
    ksqlEngine.stopAcceptingStatemens();
    EasyMock.expectLastCall();
    final QueryMetadata queryMetadata = EasyMock.mock(QueryMetadata.class);
    EasyMock.expect(ksqlEngine.getAllLiveQueries())
        .andReturn(Collections.singleton(queryMetadata));
    final KsqlTopic ksqlTopic = new KsqlTopic("bar", "bar", new KsqlJsonTopicSerDe(), true);
    final StructuredDataSource structuredDataSource = new KsqlStream("", "", EasyMock.mock(Schema.class), EasyMock.mock(
        Field.class), EasyMock.mock(TimestampExtractionPolicy.class), ksqlTopic);
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    metaStore.putSource(structuredDataSource);
    metaStore.putTopic(ksqlTopic);
    EasyMock.expect(ksqlEngine.getMetaStore()).andReturn(metaStore);


  }

  @Test
  public void shouldTerminateClusterWithoutDeleteTopicList() {
    final List<String> deleteTopicList = Collections.emptyList();
    kafkaTopicClient.deleteTopics(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(ksqlEngine, kafkaTopicClient);
    final ClusterTerminator clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine, deleteTopicList);
    clusterTerminator.terminateCluster();

    EasyMock.verify(ksqlEngine, kafkaTopicClient);
  }

  @Test
  public void shouldTerminateClusterWithDeleteTopicListWithExplicitTopicName() {
    terminateWithDeleteTopicList("bar");
  }

  @Test
  public void shouldTerminateClusterWithDeleteTopicListWithPattern() {
    terminateWithDeleteTopicList("bar*");
  }

  private void terminateWithDeleteTopicList(final String topicNamePattern) {
    final List<String> deleteTopicList = Collections.singletonList(topicNamePattern);
    kafkaTopicClient.deleteTopics(EasyMock.anyObject());
    EasyMock.expectLastCall().times(2);

    EasyMock.replay(ksqlEngine, kafkaTopicClient);

    final ClusterTerminator clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine, deleteTopicList);
    clusterTerminator.terminateCluster();

    EasyMock.verify(ksqlEngine, kafkaTopicClient);
  }

}
