/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.csv;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KSQLTopicSerDe;


public class KSQLCsvTopicSerDe extends KSQLTopicSerDe {

  public KSQLCsvTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.CSV);
  }
}
