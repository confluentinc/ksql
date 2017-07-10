/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlException;

public class KsqlTopic implements DataSource {

  final String topicName;
  final String kafkaTopicName;
  final KsqlTopicSerDe ksqlTopicSerDe;

  public KsqlTopic(final String topicName, final String kafkaTopicName, final KsqlTopicSerDe
      ksqlTopicSerDe) {
    this.topicName = topicName;
    this.kafkaTopicName = kafkaTopicName;
    this.ksqlTopicSerDe = ksqlTopicSerDe;
  }

  public KsqlTopicSerDe getKsqlTopicSerDe() {
    return ksqlTopicSerDe;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public String getTopicName() {
    return topicName;
  }

  public static DataSourceSerDe getDataSpDataSourceSerDe(String dataSourceSerdeName) {
    switch (dataSourceSerdeName) {
      case "JSON":
        return DataSourceSerDe.JSON;
      case "AVRO":
        return DataSourceSerDe.AVRO;
      case "DELIMITED":
        return DataSourceSerDe.DELIMITED;
      default:
        throw new KsqlException("DataSource Type is not supported: " + dataSourceSerdeName);
    }
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
