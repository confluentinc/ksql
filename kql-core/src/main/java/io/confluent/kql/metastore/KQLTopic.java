/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.util.KQLException;

public class KQLTopic implements DataSource {

  final String topicName;
  final String kafkaTopicName;
  final KQLTopicSerDe kqlTopicSerDe;

  public KQLTopic(String topicName, String kafkaTopicName, KQLTopicSerDe kqlTopicSerDe) {
    this.topicName = topicName;
    this.kafkaTopicName = kafkaTopicName;
    this.kqlTopicSerDe = kqlTopicSerDe;
  }

  public KQLTopicSerDe getKqlTopicSerDe() {
    return kqlTopicSerDe;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public String getTopicName() {
    return topicName;
  }

  public static DataSourceSerDe getDataSpDataSourceSerDe(String dataSourceSerdeName) {
    if (dataSourceSerdeName.equalsIgnoreCase("json")) {
      return DataSourceSerDe.JSON;
    } else if (dataSourceSerdeName.equalsIgnoreCase("avro")) {
      return DataSourceSerDe.AVRO;
    }
    throw new KQLException("DataSource Type is not supported: " + dataSourceSerdeName);
  }

  @Override
  public String getName() {
    return topicName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.KTOPIC;
  }
}
