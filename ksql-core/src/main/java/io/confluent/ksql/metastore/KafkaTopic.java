package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KafkaTopic extends AbstractDataSource {

  final String topicName;
  String alias;

  public KafkaTopic(String datasourceName, Schema schema, Field keyField,
                    DataSourceType dataSourceType, String topicName) {
    super(datasourceName, schema, keyField, dataSourceType);
    this.topicName = topicName;
  }

  @Override
  public String getName() {
    return this.dataSourceName;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  public Field getKeyField() {
    return this.keyField;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  public String getTopicName() {
    return topicName;
  }
}
