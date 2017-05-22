/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.avro;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KSQLTopicSerDe;

public class KSQLAvroTopicSerDe extends KSQLTopicSerDe {

  private final String schemaString;
  private final String schemaFilePath;

  public KSQLAvroTopicSerDe(final String schemaFilePath, final String schemaString) {
    super(StructuredDataSource.DataSourceSerDe.AVRO);
    this.schemaString = schemaString;
    this.schemaFilePath = schemaFilePath;
  }

  public String getSchemaString() {
    return schemaString;
  }

  public String getSchemaFilePath() {
    return schemaFilePath;
  }
}