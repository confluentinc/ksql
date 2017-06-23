/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;

public class KsqlAvroTopicSerDe extends KsqlTopicSerDe {

  private final String schemaString;

  public KsqlAvroTopicSerDe(final String schemaString) {
    super(StructuredDataSource.DataSourceSerDe.AVRO);
    this.schemaString = schemaString;
  }

  public String getSchemaString() {
    return schemaString;
  }
}