/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.avro;

import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.serde.KQLTopicSerDe;

public class KQLAvroTopicSerDe extends KQLTopicSerDe {

  private final String schemaString;
  private final String schemaFilePath;

  public KQLAvroTopicSerDe(final String schemaFilePath, final String schemaString) {
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
