/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.csv;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;


public class KsqlCsvTopicSerDe extends KsqlTopicSerDe {

  public KsqlCsvTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.CSV);
  }
}
