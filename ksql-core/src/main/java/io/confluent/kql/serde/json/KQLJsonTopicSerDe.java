/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.json;

import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.serde.KQLTopicSerDe;
import org.apache.kafka.connect.data.Schema;

public class KQLJsonTopicSerDe extends KQLTopicSerDe {

  Schema rowSchema;
  public KQLJsonTopicSerDe(Schema rowSchema) {
    super(StructuredDataSource.DataSourceSerDe.JSON);
    this.rowSchema = rowSchema;
  }
}
