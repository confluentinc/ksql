/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KSQLStream extends StructuredDataSource {

  public KSQLStream(final String datasourceName, final Schema schema, final Field keyField,
                    final KSQLTopic ksqlTopic) {
    super(datasourceName, schema, keyField, DataSourceType.KSTREAM, ksqlTopic);
  }
}
