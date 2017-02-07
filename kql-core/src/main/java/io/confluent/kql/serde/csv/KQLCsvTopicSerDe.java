/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.csv;

import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.serde.KQLTopicSerDe;


public class KQLCsvTopicSerDe extends KQLTopicSerDe {

  public KQLCsvTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.CSV);
  }
}
