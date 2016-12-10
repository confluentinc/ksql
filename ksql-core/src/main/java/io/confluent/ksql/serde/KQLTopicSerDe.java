package io.confluent.ksql.serde;

import io.confluent.ksql.metastore.DataSource;

public abstract class KQLTopicSerDe {

  private final DataSource.DataSourceSerDe serDe;

  protected KQLTopicSerDe(DataSource.DataSourceSerDe serDe) {
    this.serDe = serDe;
  }

  public DataSource.DataSourceSerDe getSerDe() {
    return serDe;
  }
}
