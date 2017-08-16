/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.json;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import org.apache.kafka.connect.data.Schema;

public class KsqlJsonTopicSerDe extends KsqlTopicSerDe {

  Schema rowSchema;
  public KsqlJsonTopicSerDe(Schema rowSchema) {
    super(StructuredDataSource.DataSourceSerDe.JSON);
    this.rowSchema = rowSchema;
  }

  public Schema getRowSchema() {
    return rowSchema;
  }
}