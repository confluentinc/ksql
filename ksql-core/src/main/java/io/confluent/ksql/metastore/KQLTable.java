package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLTable extends StructuredDataSource {

  final String stateStoreName;

  public KQLTable(String datasourceName, Schema schema, Field keyField,
                  KafkaTopic kafkaTopic, String stateStoreName) {
    super(datasourceName, schema, keyField, DataSourceType.KTABLE, kafkaTopic);
    this.stateStoreName = stateStoreName;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }
}
