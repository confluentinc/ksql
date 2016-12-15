package io.confluent.ksql.serde.avro;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KQLTopicSerDe;

public class KQLAvroTopicSerDe extends KQLTopicSerDe {

  private final String schemaString;

  public KQLAvroTopicSerDe(String schemaString) {
    super(StructuredDataSource.DataSourceSerDe.AVRO);
    this.schemaString = schemaString;
  }

  public String getSchemaString() {
    return schemaString;
  }
}
