/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.delimited;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;


public class KsqlDelimitedTopicSerDe extends KsqlTopicSerDe {

  public KsqlDelimitedTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.DELIMITED);
  }
}
