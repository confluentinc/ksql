/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.json;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KSQLTopicSerDe;
import org.apache.kafka.connect.data.Schema;

public class KSQLJsonTopicSerDe extends KSQLTopicSerDe {

  Schema rowSchema;
  public KSQLJsonTopicSerDe(Schema rowSchema) {
    super(StructuredDataSource.DataSourceSerDe.JSON);
    this.rowSchema = rowSchema;
  }

  public Schema getRowSchema() {
    return rowSchema;
  }
}