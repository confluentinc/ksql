/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.json;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KSQLTopicSerDe;

public class KSQLJsonTopicSerDe extends KSQLTopicSerDe {

  public KSQLJsonTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.JSON);
  }
}
