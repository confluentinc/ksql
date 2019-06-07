package io.confluent.ksql.rest.entity;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

public class KafkaTopicsListTest {


  @Test
  public void shouldBuildValidTopicList() {

    final Collection<KsqlTopic> ksqlTopics = Collections.emptyList();
    // represent the full list of topics
    final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    final TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(1, new Node(1, "", 8088),
                                                                   Collections.emptyList(), Collections.emptyList());
    topicDescriptions.put("test-topic", new TopicDescription("test-topic", false, Collections.singletonList(topicPartitionInfo)));


    /**
     * Return POJO for consumerGroupClient
     */
    final TopicPartition topicPartition = new TopicPartition("test-topic", 1);
    final KafkaConsumerGroupClientImpl.ConsumerSummary consumerSummary = new KafkaConsumerGroupClientImpl.ConsumerSummary("consumer-id");
    consumerSummary.addPartition(topicPartition);
    final KafkaConsumerGroupClientImpl.ConsumerGroupSummary consumerGroupSummary = new KafkaConsumerGroupClientImpl.ConsumerGroupSummary();
    consumerGroupSummary.addConsumerSummary(consumerSummary);



    final KafkaConsumerGroupClient consumerGroupClient = mock(KafkaConsumerGroupClient.class);
    expect(consumerGroupClient.listGroups()).andReturn(Collections.singletonList("test-topic"));
    expect(consumerGroupClient.describeConsumerGroup("test-topic")).andReturn(consumerGroupSummary);
    replay(consumerGroupClient);

    /**
     * Test
     */

    final KafkaTopicsList topicsList = KafkaTopicsList.build("statement test", ksqlTopics, topicDescriptions, new KsqlConfig(Collections.EMPTY_MAP), consumerGroupClient);

    assertThat(topicsList.getTopics().size(), equalTo(1));
    final KafkaTopicInfo first = topicsList.getTopics().iterator().next();
    assertThat(first.getConsumerGroupCount(), equalTo(1));
    assertThat(first.getConsumerCount(), equalTo(1));
    assertThat(first.getReplicaInfo().size(), equalTo(1));

  }

  @Test
  public void testSerde() throws Exception {
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final KafkaTopicsList expected = new KafkaTopicsList(
        "SHOW TOPICS;",
        ImmutableList.of(new KafkaTopicInfo("thetopic", true, ImmutableList.of(1, 2, 3), 42, 12))
    );
    final String json = mapper.writeValueAsString(expected);
    assertEquals(
        "{\"@type\":\"kafka_topics\",\"statementText\":\"SHOW TOPICS;\"," +
        "\"topics\":[{\"name\":\"thetopic\",\"registered\":true," +
        "\"replicaInfo\":[1,2,3],\"consumerCount\":42," +
        "\"consumerGroupCount\":12}]}",
        json);

    final KafkaTopicsList actual = mapper.readValue(json, KafkaTopicsList.class);
    assertEquals(expected, actual);
  }
}
