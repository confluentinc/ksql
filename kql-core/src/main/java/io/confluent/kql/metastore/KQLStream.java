/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLStream extends StructuredDataSource {

  public KQLStream(final String datasourceName, final Schema schema, final Field keyField,
                   final KQLTopic kqlTopic) {
    super(datasourceName, schema, keyField, DataSourceType.KSTREAM, kqlTopic);
  }
}
