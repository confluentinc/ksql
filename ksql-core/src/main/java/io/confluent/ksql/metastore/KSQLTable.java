/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KSQLTable extends StructuredDataSource {

  final String stateStoreName;
  final boolean isWinidowed;

  public KSQLTable(final String datasourceName, final Schema schema, final Field keyField,
                   final KSQLTopic ksqlTopic, final String stateStoreName, boolean isWinidowed) {
    super(datasourceName, schema, keyField, DataSourceType.KTABLE, ksqlTopic);
    this.stateStoreName = stateStoreName;
    this.isWinidowed = isWinidowed;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }

  public boolean isWinidowed() {
    return isWinidowed;
  }
}
