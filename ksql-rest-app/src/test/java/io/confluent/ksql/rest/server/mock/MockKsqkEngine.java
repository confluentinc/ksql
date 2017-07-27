package io.confluent.ksql.rest.server.mock;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;

public class MockKsqkEngine extends KsqlEngine {

  public MockKsqkEngine(KsqlConfig ksqlConfig,
                        KafkaTopicClient kafkaTopicClient) {
    super(ksqlConfig, kafkaTopicClient);
  }
}
