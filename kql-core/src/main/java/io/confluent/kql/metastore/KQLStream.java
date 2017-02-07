/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLStream extends StructuredDataSource {

  public KQLStream(String datasourceName, Schema schema, Field keyField,
                   KQLTopic kqlTopic) {
    super(datasourceName, schema, keyField, DataSourceType.KSTREAM, kqlTopic);
  }
}
