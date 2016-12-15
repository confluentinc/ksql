package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLStream extends StructuredDataSource {

  public KQLStream(String datasourceName, Schema schema, Field keyField,
                   KafkaTopic kafkaTopic) {
    super(datasourceName, schema, keyField, DataSourceType.KSTREAM, kafkaTopic);
  }
}
