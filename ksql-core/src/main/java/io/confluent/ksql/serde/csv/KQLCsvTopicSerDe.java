package io.confluent.ksql.serde.csv;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KQLTopicSerDe;


public class KQLCsvTopicSerDe extends KQLTopicSerDe {
  public KQLCsvTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.CSV);
  }
}
