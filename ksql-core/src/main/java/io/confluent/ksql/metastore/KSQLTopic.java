/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.util.KSQLException;

public class KSQLTopic implements DataSource {

  final String topicName;
  final String kafkaTopicName;
  final KSQLTopicSerDe ksqlTopicSerDe;

  public KSQLTopic(final String topicName, final String kafkaTopicName, final KSQLTopicSerDe
      ksqlTopicSerDe) {
    this.topicName = topicName;
    this.kafkaTopicName = kafkaTopicName;
    this.ksqlTopicSerDe = ksqlTopicSerDe;
  }

  public KSQLTopicSerDe getKsqlTopicSerDe() {
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
      default:
        throw new KSQLException("DataSource Type is not supported: " + dataSourceSerdeName);
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
