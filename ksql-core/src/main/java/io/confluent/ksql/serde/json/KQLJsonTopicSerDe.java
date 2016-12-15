package io.confluent.ksql.serde.json;

import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.KQLTopicSerDe;

/**
 * Created by hojjat on 12/8/16.
 */
public class KQLJsonTopicSerDe extends KQLTopicSerDe {

  public KQLJsonTopicSerDe() {
    super(StructuredDataSource.DataSourceSerDe.JSON);
  }
}
