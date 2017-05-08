/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde;

import io.confluent.kql.metastore.StructuredDataSource;

public abstract class KQLTopicSerDe {

  private final StructuredDataSource.DataSourceSerDe serDe;

  protected KQLTopicSerDe(StructuredDataSource.DataSourceSerDe serDe) {
    this.serDe = serDe;
  }

  public StructuredDataSource.DataSourceSerDe getSerDe() {
    return serDe;
  }
}
