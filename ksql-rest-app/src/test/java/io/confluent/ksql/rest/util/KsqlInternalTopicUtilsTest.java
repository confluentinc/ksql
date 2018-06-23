package io.confluent.ksql.rest.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class KsqlInternalTopicUtilsTest {
  private static final String TOPIC_NAME = "topic";
  private static final short NREPLICAS = 1;

  private final Map<String, ?> commandTopicConfig = ImmutableMap.of(
      TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE,
      TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    when(ksqlConfig.originals()).thenReturn(
        ImmutableMap.of(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, NREPLICAS)
    );
    when(ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)).thenReturn(NREPLICAS);
    when(topicClient.isTopicExists(TOPIC_NAME)).thenReturn(false);
  }

  @Test
  public void shouldCreateCommandTopicIfItDoesNotExist() {
    // When:
    KsqlInternalTopicUtils.ensureTopic(TOPIC_NAME, ksqlConfig, topicClient);

    // Then:
    verify(topicClient).createTopic(TOPIC_NAME, 1, NREPLICAS, commandTopicConfig);
  }

  @Test
  public void shouldNotAttemptToCreateCommandTopicIfItExists() {
    // Given:
    when(topicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    KsqlInternalTopicUtils.ensureTopic(TOPIC_NAME, ksqlConfig, topicClient);

    // Then:
    verify(topicClient, never()).createTopic(any(), anyInt(), anyShort(), anyMap());
  }

  @Test
  public void shouldEnsureCommandTopicHasInfiniteRetention() {
    // Given:
    final Map<String, Object> retentionConfig = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE
    );
    when(topicClient.isTopicExists(TOPIC_NAME)).thenReturn(true);

    // When:
    KsqlInternalTopicUtils.ensureTopic(TOPIC_NAME, ksqlConfig, topicClient);

    // Then:
    verify(topicClient.addTopicConfig(TOPIC_NAME, retentionConfig));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateCommandTopicWithNumReplicasFromConfig() {
    // Given:
    when(ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)).thenReturn((short)3);

    // When:
    KsqlInternalTopicUtils.ensureTopic(TOPIC_NAME, ksqlConfig, topicClient);

    // Then:
    verify(topicClient).createTopic(TOPIC_NAME, 1, (short) 3, commandTopicConfig);
  }

  @Test
  public void shouldNotFailIfTopicExistsOnCreation() {
    // Given:
    doThrow(new KafkaTopicExistsException("exists"))
        .when(topicClient)
        .createTopic(any(), anyInt(), anyShort(), anyMap());

    // When/Then(no throw):
    KsqlInternalTopicUtils.ensureTopic(TOPIC_NAME, ksqlConfig, topicClient);
  }
}
