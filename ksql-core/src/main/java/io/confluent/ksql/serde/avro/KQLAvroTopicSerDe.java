package io.confluent.ksql.serde.avro;

import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.serde.KQLTopicSerDe;

/**
 * Created by hojjat on 12/8/16.
 */
public class KQLAvroTopicSerDe extends KQLTopicSerDe {

  private final String schemaString;

  public KQLAvroTopicSerDe(String schemaString) {
    super(DataSource.DataSourceSerDe.AVRO);
    this.schemaString = schemaString;
  }

  public String getSchemaString() {
    return schemaString;
  }
}
