package io.confluent.ksql.rest.entity;

import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaTopicsListTest {


  @Test
  public void shouldBuildValidTopicList() {

    Collection<KsqlTopic> ksqlTopics = Collections.emptyList();
    // represent the full list of topics
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(1, new Node(1, "", 8088),
                                                                   Collections.emptyList(), Collections.emptyList());
    topicDescriptions.put("test-topic", new TopicDescription("test-topic", false, Collections.singletonList(topicPartitionInfo)));


    /**
     * Return POJO for consumerGroupClient
     */
    TopicPartition topicPartition = new TopicPartition("test-topic", 1);
    KafkaConsumerGroupClientImpl.ConsumerSummary consumerSummary = new KafkaConsumerGroupClientImpl.ConsumerSummary("consumer-id");
    consumerSummary.addPartition(topicPartition);
    KafkaConsumerGroupClientImpl.ConsumerGroupSummary consumerGroupSummary = new KafkaConsumerGroupClientImpl.ConsumerGroupSummary();
    consumerGroupSummary.addConsumerSummary(consumerSummary);



    KafkaConsumerGroupClient consumerGroupClient = mock(KafkaConsumerGroupClient.class);
    expect(consumerGroupClient.listGroups()).andReturn(Collections.singletonList("test-topic"));
    expect(consumerGroupClient.describeConsumerGroup("test-topic")).andReturn(consumerGroupSummary);
    replay(consumerGroupClient);

    /**
     * Test
     */

    KafkaTopicsList topicsList = KafkaTopicsList.build("statement test", ksqlTopics, topicDescriptions, new KsqlConfig(Collections.EMPTY_MAP), consumerGroupClient);

    assertThat(topicsList.getTopics().size(), equalTo(1));
    KafkaTopicInfo first = topicsList.getTopics().iterator().next();
    assertThat(first.getConsumerGroupCount(), equalTo(1));
    assertThat(first.getConsumerCount(), equalTo(1));
    assertThat(first.getReplicaInfo().size(), equalTo(1));

  }
}
