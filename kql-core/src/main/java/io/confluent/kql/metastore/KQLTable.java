/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLTable extends StructuredDataSource {

  final String stateStoreName;

  public KQLTable(final String datasourceName, final Schema schema, final Field keyField,
                  final KQLTopic kqlTopic, final String stateStoreName) {
    super(datasourceName, schema, keyField, DataSourceType.KTABLE, kqlTopic);
    this.stateStoreName = stateStoreName;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }
}
