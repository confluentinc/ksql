/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.json;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KQLTopicSerDe;
import org.apache.kafka.connect.data.Schema;

public class KQLJsonTopicSerDe extends KQLTopicSerDe {

  Schema rowSchema;
  public KQLJsonTopicSerDe(Schema rowSchema) {
    super(StructuredDataSource.DataSourceSerDe.JSON);
    this.rowSchema = rowSchema;
  }
}
