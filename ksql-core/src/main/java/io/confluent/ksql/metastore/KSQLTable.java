/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.metastore;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.util.SchemaUtil;

public class KSQLTable extends StructuredDataSource {

  final String stateStoreName;
  final boolean isWinidowed;

  public KSQLTable(final String datasourceName, final Schema schema, final Field keyField,
                   final Field timestampField,
                   final KSQLTopic ksqlTopic, final String stateStoreName, boolean isWinidowed) {
    super(datasourceName, schema, keyField, timestampField, DataSourceType.KTABLE, ksqlTopic);
    this.stateStoreName = stateStoreName;
    this.isWinidowed = isWinidowed;
  }

  public String getStateStoreName() {
    return stateStoreName;
  }

  public boolean isWinidowed() {
    return isWinidowed;
  }

  @Override
  public StructuredDataSource cloneWithTimeKeyColumns() {
    Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);
    return new KSQLTable(dataSourceName, newSchema, keyField, timestampField, ksqlTopic,
                         stateStoreName, isWinidowed);
  }

  @Override
  public StructuredDataSource cloneWithTimeField(String timestampfieldName) {
    Field newTimestampField = SchemaUtil.getFieldByName(schema, timestampfieldName);
    return new KSQLTable(dataSourceName, schema, keyField, newTimestampField, ksqlTopic,
                         stateStoreName, isWinidowed);
  }
}
