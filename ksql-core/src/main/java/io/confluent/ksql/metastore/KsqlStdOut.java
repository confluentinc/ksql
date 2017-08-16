/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KsqlStdOut extends StructuredDataSource {

  public static final String KSQL_STDOUT_NAME = "KSQL_STDOUT_NAME";

  public KsqlStdOut(final String datasourceName, final Schema schema, final Field keyField,
                    final Field timestampField, final DataSourceType dataSourceType) {
    super(datasourceName, schema, keyField, timestampField, dataSourceType, null);
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return null;
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    return this;
  }

  @Override
  public StructuredDataSource cloneWithTimeField(String timestampfieldName) {
    return this;
  }
}
