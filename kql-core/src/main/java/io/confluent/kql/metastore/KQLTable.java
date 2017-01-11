package io.confluent.kql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLTable extends StructuredDataSource {

  final String stateStoreName;

  public KQLTable(String datasourceName, Schema schema, Field keyField,
                  KQLTopic KQLTopic, String stateStoreName) {
    super(datasourceName, schema, keyField, DataSourceType.KTABLE, KQLTopic);
    this.stateStoreName = stateStoreName;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }
}
