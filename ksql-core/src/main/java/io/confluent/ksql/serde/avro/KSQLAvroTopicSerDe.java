/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.avro;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KSQLTopicSerDe;

public class KSQLAvroTopicSerDe extends KSQLTopicSerDe {

  private final String schemaFilePath;

  public KSQLAvroTopicSerDe(final String schemaFilePath) {
    super(StructuredDataSource.DataSourceSerDe.AVRO);
    this.schemaFilePath = schemaFilePath;
  }

  public String getSchemaFilePath() {
    return schemaFilePath;
  }
}
