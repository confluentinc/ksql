/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.serde.json;

import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.serde.KQLTopicSerDe;

public class KQLJsonTopicSerDe extends KQLTopicSerDe {

  public KQLJsonTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.JSON);
  }
}
