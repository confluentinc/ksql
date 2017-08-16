/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde;

import io.confluent.ksql.metastore.StructuredDataSource;

public abstract class KsqlTopicSerDe {

  private final StructuredDataSource.DataSourceSerDe serDe;

  protected KsqlTopicSerDe(StructuredDataSource.DataSourceSerDe serDe) {
    this.serDe = serDe;
  }

  public StructuredDataSource.DataSourceSerDe getSerDe() {
    return serDe;
  }
}
