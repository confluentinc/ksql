package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.util.KSQLException;

public class KafkaTopic extends AbstractDataSource {

  final String topicName;
  String alias;
  final KQLTopicSerDe kqlTopicSerDe;

  public KafkaTopic(String datasourceName, Schema schema, Field keyField,
                    DataSourceType dataSourceType, KQLTopicSerDe kqlTopicSerDe, String
                        topicName) {
    super(datasourceName, schema, keyField, dataSourceType);
    this.topicName = topicName;
    this.kqlTopicSerDe = kqlTopicSerDe;
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

  public KQLTopicSerDe getKqlTopicSerDe() {
    return kqlTopicSerDe;
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
    throw new KSQLException("DataSource Type is not supported: " + dataSourceSerdeName);
  }
}
